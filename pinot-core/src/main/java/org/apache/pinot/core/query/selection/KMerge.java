/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.selection;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.WindowAccumulatorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.segment.local.customobject.window.WindowAccumulator;


/**
 * Simultaneous merge of k {@link DataTable}s containing columns of {@link WindowAccumulator} values into the final
 * result set. {@link WindowAccumulator} values are replaced by actual double values of the window function.
 */
public class KMerge {
  private static final int ROW_START_INDEX = 0;
  private static final int ROW_END_INDEX = 1;

  /** A RowGroup is a group of rows that have the same value for a set of columns (sort key columns). */
  public static class RowGroup implements Comparable {
    // Table id that rows are coming from.
    int _tableId;

    // All the rows of this row group
    List<Object[]> _rows;

    // Comparator that decides whether two rows belong to the same row group. */
    Comparator<Object[]> _comparator;

    public RowGroup(int tableId, List<Object[]> rows, Comparator<Object[]> comparator) {
      _tableId = tableId;
      _rows = rows;
      _comparator = comparator;
    }

    public int getTableId() {
      return _tableId;
    }

    public Object[] getFirstRow() {
      return _rows.get(0);
    }

    public List<Object[]> getRows() {
      return _rows;
    }

    @Override
    public int compareTo(Object other) {
      if (other == this) {
        return 0;
      }

      if (!(other instanceof RowGroup)) {
        throw new IllegalStateException("Incompatible object type.");
      }
      return _comparator.compare(_rows.get(0), ((RowGroup) other).getFirstRow());
    }

    @Override
    public String toString() {
      return "(TableId: " + _tableId + ", Row Count: " + +_rows.size() + ")";
    }
  }

  private KMerge() {
  }

  /** @return merged rows from all the input data tables. */
  public static List<Object[]> merge(QueryContext queryContext, Map<ServerRoutingInstance, DataTable> dataTableMap,
      int numRowsMax) {
    //printDataTable(dataTableMap);
    DataTable[] tables = dataTableMap.values().toArray(new DataTable[0]);

    // Initialize context for merging tables with window functions.
    WindowFunctionContext windowContext =
        new WindowFunctionContext(tables.length, tables[0].getDataSchema(), queryContext);

    WindowAccumulator[] resultAccumulators = new WindowAccumulator[windowContext.getNumWindowFunctions()];
    for (int j = 0; j < windowContext.getNumWindowFunctions(); j++) {
      resultAccumulators[j] = WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(j));
    }

    // Calculate number of rows in final result set and handle WindowAccumulators for window functions
    // without ORDER BY and/or PARTITION.
    int numRowsResult = 0;
    for (int i = 0; i < windowContext.getNumTables(); i++) {
      numRowsResult += tables[i].getNumberOfRows();
      Object[] row = SelectionOperatorUtils.extractRowFromDataTable(tables[i], 0);
      int[] windowColumnsIndices = windowContext.getWindowIndices();
      int windowIndexColumn = 0;
      for (int k : windowColumnsIndices) {
        if (windowContext.isUnorderedWindowFunction(k)) {
          resultAccumulators[windowIndexColumn].apply(row[k]);
        }
        ++windowIndexColumn;
      }
    }

    // Don't exceed the maximum number of allowed rows for queries with window function.
    if (windowContext.getOrderByExpressionContexts() == null) {
      return mergeTablesWithoutWindowOrdering(tables, windowContext, resultAccumulators,
          Math.min(numRowsResult, numRowsMax));
    }

    return mergeTablesWithWindowOrdering(tables, windowContext, resultAccumulators,
        Math.min(numRowsResult, numRowsMax));
  }

  /**
   * @return merged rows from all the sorted data tables. DataTables are sorted because of presence of window function
   * with PARTITION and/or ORDER specification.
   */
  private static List<Object[]> mergeTablesWithWindowOrdering(DataTable[] tables, WindowFunctionContext windowContext,
      WindowAccumulator[] resultAccumulator, int numRowsResult) {

    // Map of column names to their index positions
    DataSchema dataSchema = tables[0].getDataSchema();
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    for (int i = 0; i < dataSchema.size(); i++) {
      columnNameToIndexMap.put(dataSchema.getColumnName(i), i);
    }

    // Starting and ending row ids for each data table
    int[][] cursors = new int[windowContext.getNumTables()][2];

    // Create a priority queue for k-way merge of k sorted tables. Add one 'Row Group' from each table to the priority
    // queue. A 'Row Group' consists up of all rows that have matching PARTITION BY and ORDER BY keys.
    int numRowsLeft = 0;
    PriorityQueue<RowGroup> pq = new PriorityQueue<>(windowContext.getNumTables());
    for (int i = 0; i < tables.length; i++) {
      numRowsLeft += tables[i].getNumberOfRows();
      addNextGroupToQueue(windowContext.getForwardComparator(), cursors, pq, i, tables);
    }
    numRowsLeft = Math.min(numRowsLeft, numRowsResult);

    // Initialize table accumulators.
    WindowAccumulator[][] tableAccumulators =
        new WindowAccumulator[windowContext.getNumTables()][windowContext.getNumWindowFunctions()];
    for (int j = 0; j < windowContext.getNumWindowFunctions(); j++) {
      for (int i = 0; i < tableAccumulators.length; i++) {
        tableAccumulators[i][j] = WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(j));
      }
    }

    boolean mergeByPartition = windowContext.getPartitionByExpressionContexts().size() > 0;
    boolean mergeByOrder = windowContext.getOrderByExpressionContexts().size() > 0;

    List<Object[]> rows = new ArrayList<>(numRowsLeft);

    List<RowGroup> groups = new ArrayList<>();
    Object[] previousFirstRow = null;

    while (!pq.isEmpty() && numRowsLeft > 0) {
      // Remove the smallest record group
      RowGroup group = pq.remove();

      // Add the next record group from the same table to the queue.
      addNextGroupToQueue(windowContext.getForwardComparator(), cursors, pq, group.getTableId(), tables);

      // Get the first row of the group and set previousFirstRow if needed.
      Object[] currentFirstRow = group.getFirstRow();
      if (previousFirstRow == null) {
        previousFirstRow = currentFirstRow;
      }

      boolean bReset = false;
      if (windowContext.getForwardComparator().compare(previousFirstRow, currentFirstRow) != 0) {
        // sort keys have changed.
        numRowsLeft = addMergedGroupsToResult(tables, windowContext, resultAccumulator, rows, groups, numRowsLeft);
        previousFirstRow = currentFirstRow;
        bReset = true;
      }

      int[] windowColumnIndices = windowContext.getWindowIndices();
      int windowColumnIndex = 0;
      for (int k: windowColumnIndices) {
        if (!windowContext.isUnorderedWindowFunction(k)) {
          if (mergeByPartition && !mergeByOrder) {
            // overwrites existing WindowAccumulator object.
            if (bReset) {
              resultAccumulator[windowColumnIndex] =
                  WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(windowColumnIndex));
            }
            resultAccumulator[windowColumnIndex].apply(currentFirstRow[k]);
          } else if (!mergeByPartition && mergeByOrder) {
            // creates new WindowAccumulator object.
            resultAccumulator[windowColumnIndex] =
                resultAccumulator[windowColumnIndex].merge((WindowAccumulator) currentFirstRow[k],
                    tableAccumulators[group.getTableId()][windowColumnIndex]);
          } else {
            if (windowContext.getPartitioningRow() == null) {
              windowContext.setPartitioningRow(currentFirstRow);
            }

            if (windowContext.rowMatchesCurrentPartition(currentFirstRow)) {
              resultAccumulator[windowColumnIndex] =
                  resultAccumulator[windowColumnIndex].merge((WindowAccumulator) currentFirstRow[k],
                      tableAccumulators[group.getTableId()][windowColumnIndex]);
            } else {
              windowContext.setPartitioningRow(currentFirstRow);
              resultAccumulator[windowColumnIndex] = (WindowAccumulator) currentFirstRow[k];

              for (int i = 0; i < tableAccumulators.length; i++) {
                tableAccumulators[i][windowColumnIndex] =
                    WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(windowColumnIndex));
              }
            }
          }
        }
        tableAccumulators[group.getTableId()][windowColumnIndex] = (WindowAccumulator) currentFirstRow[k];
        ++windowColumnIndex;
      }
      groups.add(group);
    }

    if (groups.size() > 0 && numRowsLeft > 0) {
      numRowsLeft = addMergedGroupsToResult(tables, windowContext, resultAccumulator, rows, groups, numRowsLeft);
    }

    return rows;
  }

  /**
   * @return merged rows from all the unsorted data tables. DataTables are not sorted when window functions don't
   * contain PARTITION and/or ORDER specification.
   */
  private static List<Object[]> mergeTablesWithoutWindowOrdering(DataTable[] tables,
      WindowFunctionContext windowContext, WindowAccumulator[] resultAccumulator, int maxNumRows) {
    int numRowsLeft = 0;
    for (int i = 0; i < tables.length; i++) {
      numRowsLeft += tables[i].getNumberOfRows();
    }
    numRowsLeft = Math.min(numRowsLeft, maxNumRows);

    List<Object[]> rows = new ArrayList<>(numRowsLeft);

    // Now just copy all the rows from individual tables into the final result table while ensuring that we don't
    // cross the limit on the number of rows in the merged result.
    int currentNumRows = 0;
    for (int i = 0; i < windowContext.getNumTables(); i++) {
      DataTable dataTable = tables[i];
      for (int j = 0; j < dataTable.getNumberOfRows() && currentNumRows < numRowsLeft; j++, currentNumRows++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, j);
        int[] windowColumnIndices = windowContext.getWindowIndices();
        int windowIndexColumn = 0;
        for (int k: windowColumnIndices) {
          row[k] = resultAccumulator[windowIndexColumn++].getFinalValue();
        }
        rows.add(row);
      }
    }

    return rows;
  }

  /** Adds merged group of rows into the result table. */
  private static int addMergedGroupsToResult(DataTable[] tables, WindowFunctionContext windowContext,
      WindowAccumulator[] resultAccumulator, List<Object[]> rows, List<RowGroup> groups, int remainingRows) {
    for (RowGroup group : groups) {
      List<Object[]> groupRows = group.getRows();
      for (int i = 0; i < groupRows.size(); i++) {
        Object[] row = groupRows.get(i);
        int[] windowColumnIndices = windowContext.getWindowIndices();
        int windowColumnIndex = 0;
        for (int k: windowColumnIndices) {
          row[k] = resultAccumulator[windowColumnIndex++].getFinalValue();
        }
        rows.add(row);
      }
    }
    groups.clear();

    return remainingRows;
  }

  /** Adds a row group from a given table into PriorityQueue that is used for merging all rows from all DataTables. */
  private static void addNextGroupToQueue(Comparator<Object[]> comparator, int[][] cursors, PriorityQueue<RowGroup> pq,
      int tableId, DataTable[] tables) {
    DataTable table = tables[tableId];
    // Get next set of rows with the same key.
    if (cursors[tableId][ROW_END_INDEX] < table.getNumberOfRows()) {
      // reset starting position of the block
      cursors[tableId][ROW_START_INDEX] = cursors[tableId][ROW_END_INDEX];

      // Find range of rows ( cursors[i][STARTING], cursors[i][ENDING] ] that have the same sort key.
      List<Object[]> rows = new ArrayList<>();
      Object[] firstRow = SelectionOperatorUtils.extractRowFromDataTable(table, cursors[tableId][ROW_END_INDEX]);
      while (cursors[tableId][ROW_END_INDEX] < table.getNumberOfRows()) {
        Object[] nextRow = SelectionOperatorUtils.extractRowFromDataTable(table, cursors[tableId][ROW_END_INDEX]);
        if (comparator.compare(firstRow, nextRow) == 0) {
          ++cursors[tableId][ROW_END_INDEX];
          rows.add(nextRow);
        } else {
          break;
        }
      }

      if (cursors[tableId][ROW_START_INDEX] < cursors[tableId][ROW_END_INDEX]) {
        // We have a row set to add to the queue.
        RowGroup group = new RowGroup(tableId, rows, comparator);
        pq.add(group);
      }
    }
  }
}
