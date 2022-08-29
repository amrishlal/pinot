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
package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.KMerge;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.selection.WindowFunctionContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is used to merge server-level results of a window function query into the final result set that. */
public class SelectionWindowDataTableReducer implements DataTableReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionWindowDataTableReducer.class);

  private final QueryContext _queryContext;

  SelectionWindowDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  /** Reduces data tables and sets selection results into ResultTable. */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      // For empty data table map, construct empty result using the cached data schema for selection query
      List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(_queryContext, dataSchema);
      DataSchema selectionDataSchema = SelectionOperatorUtils.getResultTableDataSchema(dataSchema, selectionColumns);
      brokerResponseNative.setResultTable(new ResultTable(selectionDataSchema, Collections.emptyList()));
    } else {
      // For data table map with more than one data tables, remove conflicting data tables
      if (dataTableMap.size() > 1) {
        List<ServerRoutingInstance> droppedServers = removeConflictingResponses(dataSchema, dataTableMap);
        if (!droppedServers.isEmpty()) {
          String errorMessage = QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses for table: " + tableName
              + " from servers: " + droppedServers + " got dropped due to data schema inconsistency.";
          LOGGER.warn(errorMessage);
          if (brokerMetrics != null) {
            brokerMetrics.addMeteredTableValue(TableNameBuilder.extractRawTableName(tableName),
                BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1L);
          }
          brokerResponseNative.addToExceptions(
              new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
        }
      }

      List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(_queryContext, dataSchema);
      List<Object[]> reducedRows = KMerge.merge(_queryContext, dataTableMap, reducerContext.getMaxWindowResultRows());
      // modify schema to reset OBJECT columns (which represent window function values) to DOUBLE.
      DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes().clone();
      for (int i = 0; i < columnDataTypes.length; i++) {
        if (columnDataTypes[i] == DataSchema.ColumnDataType.OBJECT) {
          columnDataTypes[i] = DataSchema.ColumnDataType.DOUBLE;
        }
      }

      if (reducedRows.size() >= reducerContext.getMaxWindowResultRows()) {
        brokerResponseNative.setMaxWindowResultRowsReached(true);
      }

      if (_queryContext.getOrderByExpressions() != null) {
        // If the query has ORDER BY clause, then the result set needs to be sorted by the ORDER BY columns.
        Map<String, Integer> columnNameToIndexMap = new HashMap<>();
        for (int i = 0; i < dataSchema.size(); i++) {
          columnNameToIndexMap.put(dataSchema.getColumnName(i), i);
        }

        // In place sort based on ORDER BY columns.
        reducedRows.sort(WindowFunctionContext.getForwardComparator(columnDataTypes,
            columnNameToIndexMap, _queryContext.getOrderByExpressions()));
      }

      // Apply user-specified LIMIT.
      reducedRows.subList(Math.min(reducedRows.size(), _queryContext.getLimit()), reducedRows.size()).clear();
      brokerResponseNative.setResultTable(SelectionOperatorUtils.renderResultTableWithoutOrdering(reducedRows,
          new DataSchema(dataSchema.getColumnNames(), columnDataTypes), selectionColumns));
    }
  }

  /**
   * Given a data schema, remove data tables that are not compatible with this data schema.
   * <p>Upgrade the data schema passed in to cover all remaining data schemas.
   *
   * @param dataSchema data schema.
   * @param dataTableMap map from server to data table.
   * @return list of server names where the data table got removed.
   */
  private List<ServerRoutingInstance> removeConflictingResponses(DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap) {
    List<ServerRoutingInstance> droppedServers = new ArrayList<>();
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataSchema dataSchemaToCompare = entry.getValue().getDataSchema();
      assert dataSchemaToCompare != null;
      if (!dataSchema.isTypeCompatibleWith(dataSchemaToCompare)) {
        droppedServers.add(entry.getKey());
        iterator.remove();
      } else {
        dataSchema.upgradeToCover(dataSchemaToCompare);
      }
    }
    return droppedServers;
  }
}
