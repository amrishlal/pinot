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
package org.apache.pinot.segment.local.customobject.window;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;


/** WindowAccumulator object corresponding to the MIN aggregation function used in window function specification. */
public class MinWindowAccumulator implements WindowAccumulator<MinWindowAccumulator> {
  private double _min;

  /** Public constructor used by WindowAccumulatorFactory. */
  public MinWindowAccumulator() {
    _min = Double.MAX_VALUE;
  }

  /** Constructor for internal use and testing. */
  @VisibleForTesting
  MinWindowAccumulator(double min) {
    _min = min;
  }

  /** Add a raw column value into this accumulator for final computation of window function value. */
  @Override
  public void accumulate(Object value) {
    Preconditions.checkState(value instanceof Number,
        "Value of type " + value.getClass().getName() + " cannot be accumulated by " + getClass().getName());
    _min = Math.min(_min, ((Number) value).doubleValue());
  }

  /** Apply values from input object into this accumulator. Unlike merge functions, no new accumulator is created. */
  @Override
  public void apply(Object value) {
    if (value instanceof Double) {
      _min = Math.min(_min, ((Double) value).doubleValue());
    } else if (value instanceof MinWindowAccumulator) {
      _min = Math.min(_min, ((MinWindowAccumulator) value)._min);
    } else {
      throw new IllegalStateException(
          "Value of type " + value.getClass().getName() + " cannot be applied to " + getClass().getName());
    }
  }

  /** @return new accumulator that is the sum of this accumulator and the input accumulator. */
  @Override
  public MinWindowAccumulator add(MinWindowAccumulator other) {
    return new MinWindowAccumulator(Math.min(_min, other._min));
  }

  /** @return new accumulator that is the sum of this accumulator and (current - base). */
  @Override
  public MinWindowAccumulator merge(MinWindowAccumulator other, MinWindowAccumulator base) {
    return new MinWindowAccumulator(Math.min(_min, other._min));
  }

  /** @return new accumulator that is the sum of this accumulator, (left - leftbase), and (right - rightbase). */
  @Override
  public MinWindowAccumulator merge(MinWindowAccumulator left, MinWindowAccumulator leftbase,
      MinWindowAccumulator right, MinWindowAccumulator rightbase) {
    return new MinWindowAccumulator(Math.min(_min, Math.min(left._min, right._min)));
  }

  @Override
  public double getFinalValue() {
    return _min;
  }

  @Override
  public int compareTo(MinWindowAccumulator other) {
    return Double.compare(_min, other._min);
  }

  @Override
  public String toString() {
    return String.valueOf(_min);
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
    byteBuffer.putDouble(_min);
    return byteBuffer.array();
  }

  @Nonnull
  public static MinWindowAccumulator fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static MinWindowAccumulator fromByteBuffer(ByteBuffer byteBuffer) {
    return new MinWindowAccumulator(byteBuffer.getDouble());
  }
}
