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
package org.apache.pinot.core.operator.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.WindowAccumulatorFactory;
import org.apache.pinot.core.query.aggregation.WindowAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.selection.WindowFunctionContext;
import org.apache.pinot.segment.local.customobject.window.WindowAccumulator;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * This class produces an {@link IntermediateResultsBlock} object that contains the results of a window function query
 * against a single segment. The window function columns in {@link IntermediateResultsBlock} contain
 * {@link WindowAccumulator} values. Use of {@link WindowAccumulator} values makes it easier to merge segment-level
 * results into server-level results and server-level results into broker-level results. {@link WindowAccumulator}
 * values are replaced with final result values on the Broker. To maintain sementic correctness of window function
 * queries, ORDER BY and LIMIT clause are processed on the Broker side after computation of final window function
 * results.
 */
public class SelectionOnlyWindowOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String EXPLAIN_NAME = "SELECT_WINDOW";

  // Assume that query will only select 10% of the total number of records in the segment. This ratio is used to
  // estimate the memory that initially allocated for storing row data.
  private static final int SELECTIVITY_RATIO = 10;

  private WindowFunctionContext _windowFunctionContext;
  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final List<ExpressionContext> _expressions;
  private final BlockValSet[] _blockValSets;
  private final int _maxNumRows;  // Maximum number of rows that can be processed.
  private final int _initialSize; // Initial number of rows to allocate.
  private int _numDocsScanned = 0;
  private boolean _sortNeeded = false;

  public SelectionOnlyWindowOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, TransformOperator transformOperator) {
    _indexSegment = indexSegment;
    _transformOperator = transformOperator;
    _expressions = expressions;

    _maxNumRows = queryContext.getMaxWindowSegmentRows();
    _initialSize = Math.min(_indexSegment.getSegmentMetadata().getTotalDocs(), _maxNumRows) / SELECTIVITY_RATIO;
    _windowFunctionContext = new WindowFunctionContext(transformOperator, _expressions, queryContext);
    _blockValSets = new BlockValSet[_expressions.size()];

    // Don't need to sort only when there is a single window key column that is already sorted.
    // TODO: for some reason _sortNeeded is always true. Figure out how to set _sortNeeded to true by creating an
    // index.
    List<OrderByExpressionContext> windowKey = _windowFunctionContext.getSortExpressionContexts();
    _sortNeeded =
        !(windowKey != null && windowKey.size() == 1 && SelectionOperatorUtils.isAllOrderByColumnsSorted(_indexSegment,
            windowKey));
  }

  /**
   * A window function with PARTITION and / or ORDER BY operates on a sorted result set for computing the aggregates.
   * This function will also evaluate window function without PARTITION or ORDER specifications so that we don't have
   * to make a second pass over the data.
   * @return Sorted ist of rows fetched from underlying transform operator.
   */
  private List<Object[]> getSortedBlock(AggregationExecutor aggregationExecutor) {
    List<Object[]> result = null;
    if (_sortNeeded) {
      // Create a priority queue to filter out all except the first _maxNumRows rows using a Priority queue based on
      // reverse comparator (reverse relative to the row ordering specified by the window function key).
      PriorityQueue<Object[]> maxNumRowsQueue =
          new PriorityQueue<>(_initialSize, _windowFunctionContext.getReverseComparator());
      TransformBlock transformBlock;
      while ((transformBlock = _transformOperator.nextBlock()) != null) {
        int numExpressions = _expressions.size();
        for (int i = 0; i < numExpressions; i++) {
          _blockValSets[i] = transformBlock.getBlockValueSet(_expressions.get(i));
        }

        RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);
        int numDocsToAdd = transformBlock.getNumDocs();
        for (int i = 0; i < numDocsToAdd; i++) {
          Object[] row = blockValueFetcher.getRow(i);
          if (maxNumRowsQueue.size() < _maxNumRows) {
            maxNumRowsQueue.add(row);
          } else {
            // We have reached the _maxNumRows limit.
            if (_windowFunctionContext.getReverseComparator().compare(row, maxNumRowsQueue.peek()) < 0) {
              // Replace the topmost row in priority queue with this row, so that _maxNumRows size is not exceeded.
              maxNumRowsQueue.poll();
              maxNumRowsQueue.offer(row);
            }
          }
        }

        if (aggregationExecutor != null) {
          // Aggregation is carried out over all rows, when simple window functions (such as 'avg(score) over()') are
          // present, even though only the top _maxNumRows are kept in the priority queue. This is done to ensure
          // that simple window functions will have correct result.
          aggregationExecutor.aggregate(transformBlock);
        }
        _numDocsScanned += numDocsToAdd;
      }

      // At this point the maxNumRowsQueue contains only first _maxNumRows. Extract rows in decreasing order and insert
      // the rows into an array from last to first element to get a list of first _maxNumRows rows in increasing
      // order. This produces sorted output in proper order (as per the window key order).
      Object[][] sortedLimitedData = new Object[maxNumRowsQueue.size()][];
      for (int i = sortedLimitedData.length - 1; i >= 0; i--) {
        sortedLimitedData[i] = maxNumRowsQueue.remove();
      }

      // Arrays.asList(...) uses the input array as backing store, thereby avoiding copying and resize operations that
      // would happen if an ArrayList was used.
      result = Arrays.asList(sortedLimitedData);
    } else {
      // No need to sort since the underlying column is already sorted, just put the rows directly into the result
      // array in same sorted order.
      result = new ArrayList<>(_initialSize);
      TransformBlock transformBlock;
      while ((transformBlock = _transformOperator.nextBlock()) != null) {
        int numExpressions = _expressions.size();
        for (int i = 0; i < numExpressions; i++) {
          _blockValSets[i] = transformBlock.getBlockValueSet(_expressions.get(i));
        }

        RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);
        int numDocsToAdd = Math.min(_maxNumRows - result.size(), transformBlock.getNumDocs());
        for (int i = 0; i < numDocsToAdd; i++) {
          Object[] row = blockValueFetcher.getRow(i);
          if (result.size() < _maxNumRows) {
            result.add(row);
          } else {
            // Maximum number of rows limit reached, don't process any more rows.
            break;
          }
        }

        if (aggregationExecutor != null) {
          // Aggregation is carried out over all rows, when simple window functions (such as 'avg(score) over()') are
          // present, even though only the top _maxNumRows are kept in the priority queue. This is done to ensure
          // that simple window functions will have correct result.
          aggregationExecutor.aggregate(transformBlock);
          _numDocsScanned += numDocsToAdd;
        } else {
          // Simple window functions are not present, all window functions will get evaluated only over _maxNumRows.
          if (result.size() >= _maxNumRows) {
            // Don't need to iterate through the underlying block any further.
            _numDocsScanned = result.size();
            break;
          } else {
            _numDocsScanned += numDocsToAdd;
          }
        }
      }
    }

    return result;
  }

  /**
   * This function is used when a query has only simple window functions (such as 'avg(score) over()') and there are
   * no window functions with PARTITION or ORDER specification.
   * @return Unsorted list of rows fetched from underlying transform operator.
   */
  private List<Object[]> getUnsortedBlock(AggregationExecutor aggregationExecutor) {
    ArrayList<Object[]> rows = new ArrayList<>(_initialSize);

    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      int numExpressions = _expressions.size();
      for (int i = 0; i < numExpressions; i++) {
        _blockValSets[i] = transformBlock.getBlockValueSet(_expressions.get(i));
      }
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);

      int numDocsToAdd = Math.min(_maxNumRows - rows.size(), transformBlock.getNumDocs());
      _numDocsScanned += numDocsToAdd;
      for (int i = 0; i < numDocsToAdd; i++) {
        Object[] row = blockValueFetcher.getRow(i);

        // To avoid a second pass over the rows, add reference to WindowAccumulator object to each row here. The
        // referenced value will be updated later once aggregation has been done for all rows.
        int aggregatePosition = 0;
        for (int j = 0; j < _expressions.size(); j++) {
          if (_windowFunctionContext.isWindowFunction(j)) {
            row[j] = aggregationExecutor.getResult().get(aggregatePosition++);
          }
        }
        rows.add(row);
      }

      // Aggregate values for each function aggregate function.
      aggregationExecutor.aggregate(transformBlock);
    }

    // Materialize value into the WindowAccumulator object referenced in each row.
    aggregationExecutor.getResult();
    return rows;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    List<Object[]> rows = null;

    // create AggregationExecutor for evaluating simple window functions, i.e, window functions without any partition
    // or order by clause for example 'avg (score) over()'. If there are no simple window functions, aggregationExecutor
    // will be set to null.
    WindowAggregationExecutor aggregationExecutor = _windowFunctionContext.getNumUnorderedWindowFunctions() > 0
            ? new WindowAggregationExecutor(_windowFunctionContext) : null;

    List<OrderByExpressionContext> sortExpressionContexts = _windowFunctionContext.getSortExpressionContexts();
    if (sortExpressionContexts == null) {
      // If there is no PARTITION or ORDER BY clause in any of the window function, window functions can be evaluated
      // without sorting.
      rows = getUnsortedBlock(aggregationExecutor);
      _windowFunctionContext.setMaxWindowSegmentRowsReached(rows.size() >= _maxNumRows);
      return new IntermediateResultsBlock(rows, _windowFunctionContext);
    }

    // Window functions with PARTITION and/or ORDER BY clause need sorting for evaluation.
    rows = getSortedBlock(aggregationExecutor);

    // We may also have window function without any PARTITION or ORDER BY clauses.
    List<WindowAccumulator> aggregationResult = aggregationExecutor != null ? aggregationExecutor.getResult() : null;

    int i = 0;
    int numColumns = _expressions.size();
    Object[] start = null;
    Object[] end = null;
    boolean startNewParition = true;
    while (i < rows.size()) {
      start = rows.get(i).clone();

      WindowAccumulator[] currentAggregateValues = new WindowAccumulator[numColumns];
      int windowFunctionIndex = 0;
      for (int j = 0; j < start.length; j++) {
        if (_windowFunctionContext.isWindowFunction(j)) {
          Class<? extends AggregationFunction> clazz =
              _windowFunctionContext.getAggregationFunctionClass(windowFunctionIndex);
          currentAggregateValues[j] = startNewParition ? WindowAccumulatorFactory.create(clazz)
              : WindowAccumulatorFactory.create(clazz, end[j]);
          ++windowFunctionIndex;
        }
      }

      List<OrderByExpressionContext> partitionExpressionContexts =
          _windowFunctionContext.getPartitionByExpressionContexts();
      List<OrderByExpressionContext> orderByExpressionContexts = _windowFunctionContext.getOrderByExpressionContexts();
      while (i < rows.size()) {
        Object[] next = rows.get(i);
        if (rowIsInSamePartitionAsFirstRow(start, next, partitionExpressionContexts,
            _windowFunctionContext.getColumnNameToIndexMap())) {
          if (rowIsInSameOrderAsFirstRow(start, next, orderByExpressionContexts,
              _windowFunctionContext.getColumnNameToIndexMap())) {
            // Ordering keys have not changed, so keep accumulating values.
            windowFunctionIndex = 0;
            for (int j = 0; j < numColumns; j++) {
              if (_windowFunctionContext.isWindowFunction(j)) {
                if (_windowFunctionContext.isUnorderedWindowFunction(j)) {
                  // Window function without any ORDER BY or PARITTION clauses.
                  next[j] = aggregationResult.get(windowFunctionIndex);
                } else {
                  // window function with ORDER BY and/or PARTITION clauses.
                  currentAggregateValues[j].accumulate(next[j]);
                  next[j] = currentAggregateValues[j];
                }
                ++windowFunctionIndex;
              }
            }
            end = next;
            i++;
          } else {
            startNewParition = false;
            break;
          }
        } else {
          // Partition has changed, need to start new accumulation.
          startNewParition = true;
          break;
        }
      }
    }

    _windowFunctionContext.setMaxWindowSegmentRowsReached(rows.size() >= _maxNumRows);
    return new IntermediateResultsBlock(rows, _windowFunctionContext);
  }

  private static boolean rowIsInSamePartitionAsFirstRow(Object[] start, Object[] next,
      List<OrderByExpressionContext> partitionExpressionContexts, Map<String, Integer> columnNameToIndexMap) {
    return rowIsInSameDimensionAsFirstRow(start, next, partitionExpressionContexts, columnNameToIndexMap);
  }

  private static boolean rowIsInSameOrderAsFirstRow(Object[] start, Object[] next,
      List<OrderByExpressionContext> orderExpressionContexts, Map<String, Integer> columnNameToIndexMap) {
    return rowIsInSameDimensionAsFirstRow(start, next, orderExpressionContexts, columnNameToIndexMap);
  }

  private static boolean rowIsInSameDimensionAsFirstRow(Object[] start, Object[] next,
      List<OrderByExpressionContext> dimensionExpressionContexts, Map<String, Integer> columnNameToIndexMap) {
    if (dimensionExpressionContexts == null || dimensionExpressionContexts.size() == 0) {
      return true;
    }

    for (OrderByExpressionContext column : dimensionExpressionContexts) {
      int index = columnNameToIndexMap.get(column.getExpression().toString());
      if (!start[index].equals(next[index])) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(selectList:");
    if (!_expressions.isEmpty()) {
      stringBuilder.append(_expressions.get(0));
      for (int i = 1; i < _expressions.size(); i++) {
        stringBuilder.append(", ").append(_expressions.get(i));
      }
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_transformOperator);
  }

  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }
}
