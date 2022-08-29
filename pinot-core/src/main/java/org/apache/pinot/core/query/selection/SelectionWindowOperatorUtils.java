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
import java.util.List;
import org.apache.pinot.core.query.aggregation.WindowAccumulatorFactory;
import org.apache.pinot.segment.local.customobject.window.WindowAccumulator;


/** Utility class for merging two row sets that contain window function columns. */
public class SelectionWindowOperatorUtils {
  private SelectionWindowOperatorUtils() {
  }

  public static enum ListType {
    LEFT(0),     // left table
    RIGHT(1),    // right table
    MERGED(2);   // merged table

    private int _index;

    private ListType(int index) {
      _index = index;
    }

    public int getIndex() {
      return _index;
    }
  }

  // Total number of tables (two input tables and one merged table)
  private static int _numTables = 3;

  /** @return result rows after merging leftRows with rightRows. */
  public static ArrayList<Object[]> mergeSortedWindowFunctionResults(WindowFunctionContext windowContext,
      List<Object[]> leftRows, List<Object[]> rightRows, int maxNumRows) {
    // To merge a windows function column from left and the right block, we need to keep track of the last
    // WindowAccumulator from both the left and the right intermediate result blocks along with the last
    // WindowAccumulator in the merged result.
    // WindowAccumulator[i][j]: i = 0 for left WindowAccumulator,
    //                          i = 1 for right WindowAccumulator,
    //                          i = 2 is for merged WindowAccumulator.
    // j is index position of the window function column being merged.

    // Initialize all three mergeAccumulators for all window function columns.
    WindowAccumulator[][] mergeAccumulators = new WindowAccumulator[_numTables][windowContext.getNumWindowFunctions()];
    for (int i = 0; i < mergeAccumulators.length; i++) {
      for (int j = 0; j < mergeAccumulators[0].length; j++) {
        mergeAccumulators[i][j] = WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(j));
      }
    }

    // Check if there are window functions without any PARTITION and ORDER BY clause. The result values for these
    // can be directly computed.
    if (windowContext.getNumUnorderedWindowFunctions() > 0) {
      int index = 0;
      Object[] firstLeftRowToMerge = leftRows.get(0);
      Object[] firstRightRowToMerge = rightRows.get(0);
      for (int i = 0; i < firstLeftRowToMerge.length; i++) {
        if (windowContext.isWindowFunction(i)) {
          if (windowContext.isUnorderedWindowFunction(i)) {
            WindowAccumulator leftAccumulator = (WindowAccumulator) firstLeftRowToMerge[i];
            leftAccumulator.apply(firstRightRowToMerge[i]);
            mergeAccumulators[ListType.MERGED.getIndex()][index] = leftAccumulator;
          }
          ++index;
        }
      }
    }

    // Allocate enough space (within limit of maxNumRows) to be able to merge the two segment-level results).
    ArrayList<Object[]> mergedRows = new ArrayList<>(Math.min(leftRows.size() + rightRows.size(), maxNumRows));

    // The algorithm below is the classic "merge" algorithm from "merge sort" that is used to merge two sorted lists
    // into a single result sorted list. Whenever a new row is added to the merged list, we calculate the proper value
    // of the mergeAccumulators in the merged list by using the last mergeAccumulators values for the left
    // (mergeAccumulators[0][j]), right (mergeAccumulators[1][j]), and the merged (mergeAccumulators[2][j]) lists.
    int lStart = 0;
    int rStart = 0;
    int lEnd = 0;
    int rEnd = 0;
    Comparator<Object[]> comparator = windowContext.getForwardComparator();

    while (lEnd < leftRows.size() && rEnd < rightRows.size() && mergedRows.size() < maxNumRows) {
      // Find rows (identified by lStart and lEnd index positions) in the left list that have the same sort key.
      while (lEnd + 1 < leftRows.size() && comparator.compare(leftRows.get(lEnd), leftRows.get(lEnd + 1)) == 0) {
        lEnd++;
      }
      lEnd++;

      // Find rows (identified by rStart and rEnd index positions) in the right list that have the same sort key.
      while (rEnd + 1 < rightRows.size() && comparator.compare(rightRows.get(rEnd), rightRows.get(rEnd + 1)) == 0) {
        rEnd++;
      }
      rEnd++;

      Object[] firstLeftGroupRowToMerge = leftRows.get(lStart);
      Object[] firstRightGroupRowToMerge = rightRows.get(rStart);
      int compare = comparator.compare(firstLeftGroupRowToMerge, firstRightGroupRowToMerge);
      if (compare == 0) {
        // The left and the right rows identified by indicies (lStart, lEnd) and (rStart, rEnd) have the same sort key
        // and hence considered equal for calculating window function values while merging. Merge rows from both left
        // and right lists.
        computeTwoSidedMergeAccumulators(windowContext, firstLeftGroupRowToMerge, firstRightGroupRowToMerge,
            mergeAccumulators);

        // merge all left rows
        mergeRow(mergedRows, windowContext, leftRows, lStart, lEnd, mergeAccumulators);
        lStart = lEnd;

        // merge all right rows
        mergeRow(mergedRows, windowContext, rightRows, rStart, rEnd, mergeAccumulators);
        rStart = rEnd;
      } else if (compare < 0) {
        // Compute mergeAccumulators for merging left rows.
        computeOneSidedMergeAccumulators(firstLeftGroupRowToMerge, windowContext, mergeAccumulators, ListType.LEFT);

        // Merge only the left rows identified by indicies (lStart, lEnd).
        mergeRow(mergedRows, windowContext, leftRows, lStart, lEnd, mergeAccumulators);
        lStart = lEnd;
        rEnd = rStart;
      } else {
        // Compute mergeAccumulators for merging right rows.
        computeOneSidedMergeAccumulators(firstRightGroupRowToMerge, windowContext, mergeAccumulators, ListType.RIGHT);

        // Merge only the right rows identified by indicies (rStart, rEnd).
        mergeRow(mergedRows, windowContext, rightRows, rStart, rEnd, mergeAccumulators);
        rStart = rEnd;
        lEnd = lStart;
      }
    }

    // Check if there are remaining left rows to merge.
    while (lEnd < leftRows.size() && mergedRows.size() < maxNumRows) {
      // Merge only the left rows identified by indicies (lStart, lEnd).
      computeOneSidedMergeAccumulators(leftRows.get(lEnd), windowContext, mergeAccumulators, ListType.LEFT);

      // merge all left rows
      mergeRow(mergedRows, windowContext, leftRows, lEnd, ++lEnd, mergeAccumulators);
    }

    // Check if there are remaining right rows to merge.
    while (rEnd < rightRows.size() && mergedRows.size() < maxNumRows) {
      // Merge only the right rows identified by indicies (rStart, rEnd).
      computeOneSidedMergeAccumulators(rightRows.get(rEnd), windowContext, mergeAccumulators, ListType.RIGHT);

      // merge all right rows
      mergeRow(mergedRows, windowContext, rightRows, rEnd, ++rEnd, mergeAccumulators);
    }
    return mergedRows;
  }

  static void mergeRow(ArrayList<Object[]> mergedRows, WindowFunctionContext windowFunctionContext,
      List<Object[]> rows, int rowStartIndex, int rowEndIndex, WindowAccumulator[][] mergeAccumulators) {
    while (rowStartIndex < rowEndIndex) {
      Object[] row = rows.get(rowStartIndex++);

      int columnIndex = 0;
      int[] windowIndices = windowFunctionContext.getWindowIndices();
      for (int i : windowIndices) {
        row[i] = mergeAccumulators[ListType.MERGED.getIndex()][columnIndex++];
      }
      mergedRows.add(row);
    }
  }

  static void computeOneSidedMergeAccumulators(Object[] firstGroupRowToMerge, WindowFunctionContext windowContext,
      WindowAccumulator[][] mergeAccumulators, ListType listType) {
    int windowColumnIndex = 0;
    boolean mergeByPartition = windowContext.getPartitionByExpressionContexts().size() > 0;
    boolean mergeByOrder = windowContext.getOrderByExpressionContexts().size() > 0;

    int[] windowColumnIndices = windowContext.getWindowIndices();
    for (int k: windowColumnIndices) {
      WindowAccumulator windowAccumulator = (WindowAccumulator) firstGroupRowToMerge[k];

      // Compute mergeAccumulator values.
      if (!windowContext.isUnorderedWindowFunction(k)) {
        if (mergeByPartition && !mergeByOrder) {
          // partition only merge
          mergeAccumulators[ListType.MERGED.getIndex()][windowColumnIndex] = windowAccumulator;
        } else if (!mergeByPartition && mergeByOrder) {
          // order only merge
          mergeAccumulators[ListType.MERGED.getIndex()][windowColumnIndex] =
              mergeAccumulators[ListType.MERGED.getIndex()][windowColumnIndex].merge(windowAccumulator,
                  mergeAccumulators[listType.getIndex()][windowColumnIndex]);
        } else {
          // Keep track of current partition.
          if (windowContext.getPartitioningRow() == null) {
            windowContext.setPartitioningRow(firstGroupRowToMerge);
          }

          if (windowContext.rowMatchesCurrentPartition(firstGroupRowToMerge)) {
            mergeAccumulators[ListType.MERGED.getIndex()][windowColumnIndex] =
                mergeAccumulators[ListType.MERGED.getIndex()][windowColumnIndex].merge(windowAccumulator,
                    mergeAccumulators[listType.getIndex()][windowColumnIndex]);
          } else {
            // set new partition
            windowContext.setPartitioningRow(firstGroupRowToMerge);
            mergeAccumulators[ListType.MERGED.getIndex()][windowColumnIndex] = windowAccumulator;
            mergeAccumulators[ListType.LEFT.getIndex()][windowColumnIndex] =
                WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(windowColumnIndex));
            mergeAccumulators[ListType.RIGHT.getIndex()][windowColumnIndex] =
                WindowAccumulatorFactory.create(windowContext.getAggregationFunctionClass(windowColumnIndex));
          }
        }

        // Compute updated mergeAccumulators value.
        mergeAccumulators[listType.getIndex()][windowColumnIndex] = windowAccumulator;
      }
      windowColumnIndex++;
    }
  }

  static void computeTwoSidedMergeAccumulators(WindowFunctionContext windowFunctionContext,
      Object[] firstLeftGroupRowToMerge, Object[] firstRightGroupRowToMerge, WindowAccumulator[][] mergeAccumulators) {
    int columnIndex = 0;
    boolean mergeByPartition = windowFunctionContext.getPartitionByExpressionContexts().size() > 0;
    boolean mergeByOrder = windowFunctionContext.getOrderByExpressionContexts().size() > 0;

    int[] windowColumnIndices = windowFunctionContext.getWindowIndices();
    for (int k: windowColumnIndices) {
      WindowAccumulator leftAccumulator = (WindowAccumulator) firstLeftGroupRowToMerge[k];
      WindowAccumulator rightAccumulator = (WindowAccumulator) firstRightGroupRowToMerge[k];

      // Compute merged mergeAccumulators values.
      if (!windowFunctionContext.isUnorderedWindowFunction(k)) {
        if (mergeByPartition && !mergeByOrder) {
          // partition only merge.
          mergeAccumulators[ListType.MERGED.getIndex()][columnIndex] = leftAccumulator.add(rightAccumulator);
        } else if (!mergeByPartition && mergeByOrder) {
          // order only merge.
          mergeAccumulators[ListType.MERGED.getIndex()][columnIndex] =
              mergeAccumulators[ListType.MERGED.getIndex()][columnIndex].merge(leftAccumulator,
                  mergeAccumulators[ListType.LEFT.getIndex()][columnIndex], rightAccumulator,
                  mergeAccumulators[ListType.RIGHT.getIndex()][columnIndex]);
        } else {
          // Keep track of current partition.
          if (windowFunctionContext.getPartitioningRow() == null) {
            windowFunctionContext.setPartitioningRow(firstLeftGroupRowToMerge);
          }

          if (windowFunctionContext.rowMatchesCurrentPartition(firstLeftGroupRowToMerge)) {
            mergeAccumulators[ListType.MERGED.getIndex()][columnIndex] =
                mergeAccumulators[ListType.MERGED.getIndex()][columnIndex].merge(leftAccumulator,
                    mergeAccumulators[ListType.LEFT.getIndex()][columnIndex], rightAccumulator,
                    mergeAccumulators[ListType.RIGHT.getIndex()][columnIndex]);
          } else {
            // set new partition
            windowFunctionContext.setPartitioningRow(firstLeftGroupRowToMerge);
            mergeAccumulators[ListType.MERGED.getIndex()][columnIndex] = leftAccumulator.add(rightAccumulator);
          }
        }

        // Compute updated left and right mergeAccumulators values.
        mergeAccumulators[ListType.LEFT.getIndex()][columnIndex] = leftAccumulator;
        mergeAccumulators[ListType.RIGHT.getIndex()][columnIndex] = rightAccumulator;
      }
      columnIndex++;
    }
  }
}
