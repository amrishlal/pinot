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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;


/** Transform Function for SQL Window Function specification. */
public class WindowTransformFunction extends BaseTransformFunction {
  private static final String FUNCTION_NAME = "window";

  TransformFunction _source;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    //_source is null when the aggregation function of the window function does not have any identifier arguments. For
    //example "count(*) over()"
    _source = arguments.size() != 0 ? arguments.get(0) : null;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _source != null ? _source.getResultMetadata()
        : new TransformResultMetadata(FieldSpec.DataType.DOUBLE, true, false);
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_source == null) {
      return new int[projectionBlock.getNumDocs()];
    }

    return _source.transformToIntValuesSV(projectionBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_source == null) {
      return new long[projectionBlock.getNumDocs()];
    }

    return _source.transformToLongValuesSV(projectionBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_source == null) {
      return new float[projectionBlock.getNumDocs()];
    }

    return _source.transformToFloatValuesSV(projectionBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_source == null) {
      return new double[projectionBlock.getNumDocs()];
    }

    return _source.transformToDoubleValuesSV(projectionBlock);
  }
}
