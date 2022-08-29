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
package org.apache.pinot.core.operator.combine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.selection.SelectionWindowOperatorUtils;
import org.apache.pinot.core.query.selection.WindowFunctionContext;
import org.apache.pinot.segment.local.customobject.window.WindowAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to merge two {@link IntermediateResultsBlock} with {@link WindowAccumulator} columns together
 * into a single {@link IntermediateResultsBlock} with merged {@link WindowAccumulator} column. The merge is done in
 * way that ensures the correctness of {@link WindowAccumulator} values after the merge (@see
 * <a href="https://bit.ly/3JQmz5E">window function design doc</a> for more details on how merge is carried out).
 */
public class SelectionOnlyWindowCombineOperator extends BaseCombineOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionOnlyWindowCombineOperator.class);
  private static final String OPERATOR_NAME = "SelectionOnlyCombineOperator";
  private static final String EXPLAIN_NAME = "COMBINE_SELECT_WINDOW";

  public SelectionOnlyWindowCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(operators, queryContext, executorService);
    List<ExpressionContext> expressionContexts = queryContext.getSelectExpressions();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    DataSchema mergedDataSchema = mergedBlock.getWindowFunctionContext().getDataSchema();
    DataSchema dataSchemaToMerge = blockToMerge.getWindowFunctionContext().getDataSchema();
    assert mergedDataSchema != null && dataSchemaToMerge != null;
    if (!mergedDataSchema.equals(dataSchemaToMerge)) {
      String errorMessage =
          String.format("Data schema mismatch between merged block: %s and block to merge: %s, drop block to merge",
              mergedDataSchema, dataSchemaToMerge);
      // NOTE: This is segment level log, so log at debug level to prevent flooding the log.
      LOGGER.debug(errorMessage);
      mergedBlock.addToProcessingExceptions(
          QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, errorMessage));
      return;
    }

    if (mergedBlock.getWindowFunctionContext().getOrderByExpressionContexts() == null) {
      // Merge two IntermediateResultBlock blocks when there is no PARTITION or ORDER BY specified
      WindowFunctionContext windowFunctionContext = mergedBlock.getWindowFunctionContext();
      Collection<Object[]> leftRows = mergedBlock.getSelectionResult();
      Collection<Object[]> rightRows = blockToMerge.getSelectionResult();
      assert leftRows != null && rightRows != null;

      // update aggregate values based on one row of data.
      Object[] firstLeftRow = leftRows.iterator().next();
      Object[] firstRightRow = rightRows.iterator().next();

      int[] windowColumnIndices = windowFunctionContext.getWindowIndices();
      for (int i : windowColumnIndices) {
        if (firstLeftRow[i] instanceof WindowAccumulator) {
          // Update left side window accumulator with right side window accumulator.
          WindowAccumulator leftAccumulator = (WindowAccumulator) firstLeftRow[i];
          leftAccumulator.apply(firstRightRow[i]);
        }
      }

      // Iterate through all the right rows to reset their avgPair references. We need to do this so that merge can be
      // done easily again on the Broker side.
      Iterator<Object[]> iterator = rightRows.iterator();
      while (iterator.hasNext()) {
        Object[] rightRow = iterator.next();
        for (int i : windowColumnIndices) {
          rightRow[i] = firstLeftRow[i];
        }
      }

      int numMaxRows = _queryContext.getMaxWindowServerRows();
      SelectionOperatorUtils.mergeWithoutOrdering(leftRows, rightRows, numMaxRows);
      windowFunctionContext.setMaxWindowServerRowsReached(leftRows.size() >= numMaxRows);
    } else {
      // Given two sorted IntermediateResultBlock, this function will merge the results into a single sorted
      // IntermediateResultBlock.
      WindowFunctionContext windowFunctionContext = mergedBlock.getWindowFunctionContext();
      int maxNumRows = _queryContext.getMaxWindowServerRows();
      ArrayList<Object[]> mergedRows =
          SelectionWindowOperatorUtils.mergeSortedWindowFunctionResults(windowFunctionContext,
              (List<Object[]>) mergedBlock.getSelectionResult(),
              (List<Object[]>) blockToMerge.getSelectionResult(), maxNumRows);
      windowFunctionContext.setMaxWindowServerRowsReached(mergedRows.size() >= maxNumRows);

      // Replace left block with merged rows.
      mergedBlock.setSelectionResult(mergedRows);
    }
  }
}
