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
package org.apache.pinot.core.query.aggregation;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AvgAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MaxAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.segment.local.customobject.window.AvgWindowAccumulator;
import org.apache.pinot.segment.local.customobject.window.CountWindowAccumulator;
import org.apache.pinot.segment.local.customobject.window.MaxWindowAccumulator;
import org.apache.pinot.segment.local.customobject.window.MinWindowAccumulator;
import org.apache.pinot.segment.local.customobject.window.SumWindowAccumulator;
import org.apache.pinot.segment.local.customobject.window.WindowAccumulator;


/**
 * This class stores the mapping between {@link AggregationFunction} and their corresponding {@link WindowAccumulator}.
 * The mapping is used to instantiate a WindowAccumulator object for a given AggregationFunction. Each window
 * aggregation function must have its own accumulator class derived from {@link WindowAccumulator}. Each
 * {@link WindowAccumulator} derived class must also define serialization functions in
 * {@link org.apache.pinot.core.common.ObjectSerDeUtils} class.
 */
public class WindowAccumulatorFactory {

  private static Map<Class<? extends AggregationFunction>, Class<? extends WindowAccumulator>> _functionAccumulatorMap =
      new HashMap<>();

  static {
    _functionAccumulatorMap.put(AvgAggregationFunction.class, AvgWindowAccumulator.class);
    _functionAccumulatorMap.put(SumAggregationFunction.class, SumWindowAccumulator.class);
    _functionAccumulatorMap.put(MinAggregationFunction.class, MinWindowAccumulator.class);
    _functionAccumulatorMap.put(MaxAggregationFunction.class, MaxWindowAccumulator.class);
    _functionAccumulatorMap.put(CountAggregationFunction.class, CountWindowAccumulator.class);
  }

  private WindowAccumulatorFactory() {
  }

  /** @return {@link WindowAccumulator} object corresponding to the given {@link AggregationFunction}. */
  public static WindowAccumulator create(Class<? extends AggregationFunction> aggregationFunctionClass) {
    try {
      Class<? extends WindowAccumulator> accumulatorClass = _functionAccumulatorMap.get(aggregationFunctionClass);
      return accumulatorClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate WindowAccumulator for " + aggregationFunctionClass.getName(), e);
    }
  }

  /** @return {@link WindowAccumulator} object corresponding to the given {@link AggregationFunction}. */
  public static WindowAccumulator create(Class<? extends AggregationFunction> aggregationFunctionClass, Object value) {
    WindowAccumulator accumulator = create(aggregationFunctionClass);
    accumulator.apply(value);

    return accumulator;
  }
}
