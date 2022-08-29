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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.ByteArray;

/** This class maintains the current state of window function computation. */
public class WindowFunctionContext {

  // Number of tables that we need to merge
  int _numTables;

  // Number of columns in the table
  int _numColumns;

  // Number of window function columns in the table
  int _numWindowFunctions;

  // Number of window functions without ordering (i.e without PARTIITON or ORDER BY)
  int _numUnorderedWindowFunctions;

  // Array specifying whether a particular column is Window Function
  boolean[] _isWindowFunction;

  // Array specifying whether a particular column is Window Function without ordering.
  boolean[] _isUnorderedWindowFunction;

  // AggregationFunction associated with each window function column.
  List<AggregationFunction> _aggregationFunctions;

  // Partitioning columns for all the Window functions.
  List<OrderByExpressionContext> _partitionByExpressionContexts;

  // Ordering columns for all the Window functions.
  List<OrderByExpressionContext> _orderByExpressionContexts;

  // Sorting columns (usually this is just going to be combination of partitioning and ordering columns)
  List<OrderByExpressionContext> _sortExpressionContexts;

  // Current partitioning row (set and used only during runtime).
  Object[] _partitioningRow;

  DataSchema _dataSchema;

  // Comparator used for determining if two rows have the same sort key.
  Comparator<Object[]> _forwardComparator = null;
  Comparator<Object[]> _reverseComparator = null;

  // Column name to column index position in the result array.
  Map<String, Integer> _columnNameToIndexMap;

  // Window function column indicies
  int[] _windowIndices;

  // Has "Maximum Window Segment Rows" limit been reached.
  boolean _maxWindowSegmentRowsReached;

  // Has "Maximum Window Server Rows" limit been reached.
  boolean _maxWindowServerRowsReached;

  public WindowFunctionContext(TransformOperator transformOperator, List<ExpressionContext> expressionContexts,
      QueryContext queryContext) {
    _numTables = 1;
    _numColumns = expressionContexts.size();
    _isWindowFunction = new boolean[_numColumns];
    _isUnorderedWindowFunction = new boolean[_numColumns];
    _columnNameToIndexMap = new HashMap<>();

    _aggregationFunctions = new ArrayList<>();
    String[] columnNames = new String[expressionContexts.size()];
    DataSchema.ColumnDataType[] resultColumnDataTypes = new DataSchema.ColumnDataType[_numColumns];
    for (int i = 0; i < _numColumns; i++) {
      ExpressionContext expression = expressionContexts.get(i);
      TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
      resultColumnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(), expressionMetadata.isSingleValue());
      columnNames[i] = expression.toString();
      _columnNameToIndexMap.put(columnNames[i], i);
      _isWindowFunction[i] =
          expression.getType() == ExpressionContext.Type.FUNCTION && expression.getFunction().getFunctionName()
              .equalsIgnoreCase(TransformFunctionType.WINDOW.getName());
      if (_isWindowFunction[i]) {
        ++_numWindowFunctions;
        resultColumnDataTypes[i] = DataSchema.ColumnDataType.OBJECT;

        // Get all the arguments of the window function.
        List<ExpressionContext> windowFunctionArguments = expression.getFunction().getArguments();

        // Add aggregation function to list of aggregation functions of window functions.
        _aggregationFunctions.add(
            AggregationFunctionFactory.getAggregationFunction(windowFunctionArguments.get(0).getFunction(),
                queryContext));

        FunctionContext partitionByFunctionContext = windowFunctionArguments.get(1).getFunction();
        FunctionContext orderByFunctionContext = windowFunctionArguments.get(2).getFunction();
        if (partitionByFunctionContext.getArguments().size() > 0 || orderByFunctionContext.getArguments().size() > 0) {
          if (_partitionByExpressionContexts == null && _orderByExpressionContexts == null) {
            if (_partitionByExpressionContexts == null) {
              _partitionByExpressionContexts = new ArrayList<>();
              addToOrderByExpressionContextList(_partitionByExpressionContexts,
                  partitionByFunctionContext.getArguments());
            }

            if (_orderByExpressionContexts == null) {
              _orderByExpressionContexts = new ArrayList<>();
              addToOrderByExpressionContextList(_orderByExpressionContexts, orderByFunctionContext.getArguments());
            }
          } else {
            checkPartitionAndOrdering(_partitionByExpressionContexts, partitionByFunctionContext,
                _orderByExpressionContexts, orderByFunctionContext);
          }
        } else {
          ++_numUnorderedWindowFunctions;
          _isUnorderedWindowFunction[i] = true;
        }
      }
    }


    _windowIndices = new int[_numWindowFunctions];
    int position = 0;
    for (int i = 0; i < _numColumns; i++) {
      if (isWindowFunction(i)) {
        _windowIndices[position++] = i;
      }
    }

    _dataSchema = new DataSchema(columnNames, resultColumnDataTypes);

    if (_partitionByExpressionContexts != null || _orderByExpressionContexts != null) {
      // PARTITION and/or ORDER clauses are present in the window function; hence, we will need to sort.
      _sortExpressionContexts = new ArrayList<>();
      if (_partitionByExpressionContexts != null) {
        _sortExpressionContexts.addAll(_partitionByExpressionContexts);
      }
      if (_orderByExpressionContexts != null) {
        _sortExpressionContexts.addAll(_orderByExpressionContexts);
      }

      _forwardComparator = getForwardComparator(resultColumnDataTypes, _columnNameToIndexMap, _sortExpressionContexts);
      _reverseComparator = getReverseComparator(resultColumnDataTypes, _columnNameToIndexMap, _sortExpressionContexts);
    }

    _partitioningRow = null;
    _maxWindowSegmentRowsReached = false;
    _maxWindowServerRowsReached = false;
  }

  public WindowFunctionContext(int numTables, DataSchema dataSchema, QueryContext queryContext) {
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(queryContext, null);
    _numTables = numTables;
    _numColumns = expressions.size();
    _isWindowFunction = new boolean[_numColumns];
    _isUnorderedWindowFunction = new boolean[_numColumns];
    _columnNameToIndexMap = new HashMap<>();

    _aggregationFunctions = new ArrayList<>();
    for (int i = 0; i < _numColumns; i++) {
      ExpressionContext expression = expressions.get(i);
      _columnNameToIndexMap.put(dataSchema.getColumnName(i), i);
      _isWindowFunction[i] =
          expression.getType() == ExpressionContext.Type.FUNCTION && expression.getFunction().getFunctionName()
              .equalsIgnoreCase(TransformFunctionType.WINDOW.getName());

      if (_isWindowFunction[i]) {
        ++_numWindowFunctions;
        // Get all the arguments of the window function.
        List<ExpressionContext> windowFunctionArguments = expression.getFunction().getArguments();

        // Add aggregation function to list of aggregation functions of window functions.
        _aggregationFunctions.add(
            AggregationFunctionFactory.getAggregationFunction(windowFunctionArguments.get(0).getFunction(),
                queryContext));

        FunctionContext partitionByFunctionContext = windowFunctionArguments.get(1).getFunction();
        FunctionContext orderByFunctionContext = windowFunctionArguments.get(2).getFunction();
        if (partitionByFunctionContext.getArguments().size() > 0 || orderByFunctionContext.getArguments().size() > 0) {
          if (_partitionByExpressionContexts == null && _orderByExpressionContexts == null) {
            if (_partitionByExpressionContexts == null) {
              _partitionByExpressionContexts = new ArrayList<>();
              addToOrderByExpressionContextList(_partitionByExpressionContexts,
                  partitionByFunctionContext.getArguments());
            }

            if (_orderByExpressionContexts == null) {
              _orderByExpressionContexts = new ArrayList<>();
              addToOrderByExpressionContextList(_orderByExpressionContexts, orderByFunctionContext.getArguments());
            }
          } else {
            checkPartitionAndOrdering(_partitionByExpressionContexts, partitionByFunctionContext,
                _orderByExpressionContexts, orderByFunctionContext);
          }
        } else {
          ++_numUnorderedWindowFunctions;
          _isUnorderedWindowFunction[i] = true;
        }
      }
    }

    _windowIndices = new int[_numWindowFunctions];
    int position = 0;
    for (int i = 0; i < _numColumns; i++) {
      if (isWindowFunction(i)) {
        _windowIndices[position++] = i;
      }
    }

    // Row comparator to determine if two rows have the same key
    _dataSchema = dataSchema;
    if (_partitionByExpressionContexts != null || _orderByExpressionContexts != null) {
      // PARTITION and/or ORDER clauses are present in the window function; hence, we will need to sort.
      _sortExpressionContexts = new ArrayList<>();
      if (_partitionByExpressionContexts != null) {
        _sortExpressionContexts.addAll(_partitionByExpressionContexts);
      }
      if (_orderByExpressionContexts != null) {
        _sortExpressionContexts.addAll(_orderByExpressionContexts);
      }
      _forwardComparator =
          getForwardComparator(dataSchema.getColumnDataTypes(), _columnNameToIndexMap, _sortExpressionContexts);
    }

    _partitioningRow = null;
    _maxWindowSegmentRowsReached = false;
    _maxWindowServerRowsReached = false;
  }

  /** Throw error if partition and/or order by clause of the window function is different. */
  private static void checkPartitionAndOrdering(List<OrderByExpressionContext> partitionByExpressionContexts,
      FunctionContext partitionByFunctionContext, List<OrderByExpressionContext> orderByExpressionContexts,
      FunctionContext orderByFunctionContext) {
    List<OrderByExpressionContext> currentPartitionByExpressionContexts = new ArrayList<>();
    addToOrderByExpressionContextList(currentPartitionByExpressionContexts, partitionByFunctionContext.getArguments());

    List<OrderByExpressionContext> currentOrderByExpressionContexts = new ArrayList<>();
    addToOrderByExpressionContextList(currentOrderByExpressionContexts, orderByFunctionContext.getArguments());

    // TODO: Future enhancement: support multiple window functions in a query where ORDER BY and PARTITION on one
    // window function are subsets of ORDER BY PARTITION clause on another window function.
    if (currentPartitionByExpressionContexts.size() > 0 || currentOrderByExpressionContexts.size() > 0) {
      Preconditions.checkState(currentPartitionByExpressionContexts.equals(partitionByExpressionContexts),
          "PARTITION BY clause must be the same across all window functions in the select list");

      Preconditions.checkState(currentOrderByExpressionContexts.equals(orderByExpressionContexts),
          "ORDER BY clause must be same across all window functions in the select list");
    }
  }

  /**
   * @param columnDataTypes Datatype of each column of the row.
   * @param columnNameToIndexMap Map of column name to index position of the column in the row.
   * @param sortExpressionContexts Columns what would be used to compare two rows.
   * @return A comparator that orders record by the sortExpressionContexts ordering criteria.
   */
  public static Comparator<Object[]> getForwardComparator(DataSchema.ColumnDataType[] columnDataTypes,
      Map<String, Integer> columnNameToIndexMap, List<OrderByExpressionContext> sortExpressionContexts) {
    int[] indexes = new int[sortExpressionContexts.size()];
    boolean[] orders = new boolean[sortExpressionContexts.size()];
    for (int i = 0; i < sortExpressionContexts.size(); i++) {
      OrderByExpressionContext sortExpressionContext = sortExpressionContexts.get(i);
      indexes[i] = columnNameToIndexMap.get(sortExpressionContext.getExpression().toString());
      orders[i] = sortExpressionContext.isAsc();
    }
    return (o2, o1) -> {
      return compare(indexes, orders, o1, o2, columnDataTypes);
    };
  }

  /**
   * @param columnDataTypes Datatype of each column of the row.
   * @param columnNameToIndexMap Map of column name to index position of the column in the row.
   * @param sortExpressionContexts Columns what would be used to compare two rows.
   * @return A comparator that orders record by the reverse of sortExpressionContexts ordering criteria.
   */
  public static Comparator<Object[]> getReverseComparator(DataSchema.ColumnDataType[] columnDataTypes,
      Map<String, Integer> columnNameToIndexMap, List<OrderByExpressionContext> sortExpressionContexts) {
    int[] indexes = new int[sortExpressionContexts.size()];
    boolean[] orders = new boolean[sortExpressionContexts.size()];
    for (int i = 0; i < sortExpressionContexts.size(); i++) {
      OrderByExpressionContext sortExpressionContext = sortExpressionContexts.get(i);
      indexes[i] = columnNameToIndexMap.get(sortExpressionContext.getExpression().toString());
      orders[i] = sortExpressionContext.isAsc();
    }
    return (o1, o2) -> {
      return compare(indexes, orders, o1, o2, columnDataTypes);
    };
  }

  private static int compare(int[] indexes, boolean[] orders, Object[] o1, Object[] o2,
      DataSchema.ColumnDataType[] columnDataTypes) {
    if (indexes != null) {
      for (int i = 0; i < indexes.length; i++) {
        Comparable v1 = (Comparable) o1[indexes[i]];
        Comparable v2 = (Comparable) o2[indexes[i]];
        int result = v1.compareTo(v2);
        if (result != 0) {
          return orders[i] ? -result : result;
        }
      }
    } else {
      // Rows are considered equal to each other in this case.
    }
    return 0;
  }

  static List<OrderByExpressionContext> getOrderByExpressionContextList(List<ExpressionContext> orderByList) {
    List<OrderByExpressionContext> orderByExpressions = null;
    if (CollectionUtils.isNotEmpty(orderByList)) {
      // Deduplicate the order-by expressions
      orderByExpressions = new ArrayList<>(orderByList.size());
      Set<ExpressionContext> expressionContextSet = new HashSet<>();
      for (ExpressionContext orderBy : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        FunctionContext functionContext = orderBy.getFunction();
        ExpressionContext expressionContext = functionContext.getArguments().get(0);
        if (expressionContextSet.add(expressionContext)) {
          boolean isAsc = functionContext.getFunctionName().equalsIgnoreCase("ASC");
          orderByExpressions.add(new OrderByExpressionContext(expressionContext, isAsc));
        }
      }
    }

    return orderByExpressions;
  }

  private static void addToOrderByExpressionContextList(List<OrderByExpressionContext> orderByExpressions,
      List<ExpressionContext> orderByList) {
    if (CollectionUtils.isNotEmpty(orderByList)) {
      // Deduplicate the order-by expressions
      Set<ExpressionContext> expressionContextSet = new HashSet<>();
      for (ExpressionContext orderBy : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        FunctionContext functionContext = orderBy.getFunction();
        ExpressionContext expressionContext = functionContext.getArguments().get(0);
        if (expressionContextSet.add(expressionContext)) {
          boolean isAsc = functionContext.getFunctionName().equalsIgnoreCase("ASC");
          orderByExpressions.add(new OrderByExpressionContext(expressionContext, isAsc));
        }
      }
    }
  }

  public int getNumTables() {
    return _numTables;
  }

  public int getNumColumns() {
    return _numColumns;
  }

  public int getNumWindowFunctions() {
    return _numWindowFunctions;
  }

  public int getNumUnorderedWindowFunctions() {
    return _numUnorderedWindowFunctions;
  }

  public boolean isWindowFunction(int columnIndex) {
    return _isWindowFunction[columnIndex];
  }

  public boolean isUnorderedWindowFunction(int columnIndex) {
    return _isUnorderedWindowFunction[columnIndex];
  }

  public int[] getWindowIndices() {
    return _windowIndices;
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  public Comparator<Object[]> getForwardComparator() {
    return _forwardComparator;
  }

  public Comparator<Object[]> getReverseComparator() {
    return _reverseComparator;
  }

  public Map<String, Integer> getColumnNameToIndexMap() {
    return _columnNameToIndexMap;
  }

  public List<AggregationFunction> getAggregationFunctions() {
    return _aggregationFunctions;
  }

  public Class<? extends AggregationFunction> getAggregationFunctionClass(int i) {
    return _aggregationFunctions.get(i).getClass();
  }

  public List<OrderByExpressionContext> getPartitionByExpressionContexts() {
    return _partitionByExpressionContexts;
  }

  public List<OrderByExpressionContext> getOrderByExpressionContexts() {
    return _orderByExpressionContexts;
  }

  public List<OrderByExpressionContext> getSortExpressionContexts() {
    return _sortExpressionContexts;
  }

  public Object[] getPartitioningRow() {
    return _partitioningRow;
  }

  public void setPartitioningRow(Object[] partitioningRow) {
    _partitioningRow = partitioningRow;
  }

  public boolean rowMatchesCurrentPartition(Object[] row) {
    if (_partitioningRow == null || row == null) {
      // No partitioning row is specified, so all rows fall into the same partition.
      return true;
    }

    if (_partitioningRow.length != row.length) {
      // Row doesn't have the same number of columns, so cannot be part of this partition.
      return false;
    }

    for (OrderByExpressionContext column : _partitionByExpressionContexts) {
      int index = _columnNameToIndexMap.get(column.getExpression().toString());
      if (!_partitioningRow[index].equals(row[index])) {
        return false;
      }
    }

    return true;
  }

  public boolean isMaxWindowSegmentRowsReached() {
    return _maxWindowSegmentRowsReached;
  }

  public void setMaxWindowSegmentRowsReached(boolean maxWindowSegmentRowsReached) {
    _maxWindowSegmentRowsReached = maxWindowSegmentRowsReached;
  }

  public boolean isMaxWindowServerRowsReached() {
    return _maxWindowServerRowsReached;
  }

  public void setMaxWindowServerRowsReached(boolean maxWindowServerRowsReached) {
    _maxWindowServerRowsReached = maxWindowServerRowsReached;
  }
}
