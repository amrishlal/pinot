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


/** WindowAccumulator object corresponding to the MAX aggregation function used in window function specification. */
public class MaxWindowAccumulator implements WindowAccumulator<MaxWindowAccumulator> {
  private double _max;

  /** Public constructor used by WindowAccumulatorFactory. */
  public MaxWindowAccumulator() {
    _max = Double.MIN_VALUE;
  }

  /** Constructor for internal use and testing. */
  @VisibleForTesting
  MaxWindowAccumulator(double min) {
    _max = min;
  }

  /** Add a raw column value into this accumulator for final computation of window function value. */
  @Override
  public void accumulate(Object value) {
    Preconditions.checkState(value instanceof Number,
        "Value of type " + value.getClass().getName() + " cannot be accumulated by " + getClass().getName());
    _max = Math.max(_max, ((Number) value).doubleValue());
  }

  /** Apply values from input object into this accumulator. Unlike merge functions, no new accumulator is created. */
  @Override
  public void apply(Object value) {
    if (value instanceof Double) {
      _max = Math.max(_max, ((Double) value).doubleValue());
    } else if (value instanceof MaxWindowAccumulator) {
      _max = Math.max(_max, ((MaxWindowAccumulator) value)._max);
    } else {
      throw new IllegalStateException(
          "Value of type " + value.getClass().getName() + " cannot be applied to " + getClass().getName());
    }
  }

  /** @return new accumulator that is the sum of this accumulator and the input accumulator. */
  @Override
  public MaxWindowAccumulator add(MaxWindowAccumulator other) {
    return new MaxWindowAccumulator(Math.max(_max, other._max));
  }

  /** @return new accumulator that is the sum of this accumulator and (current - base). */
  @Override
  public MaxWindowAccumulator merge(MaxWindowAccumulator other, MaxWindowAccumulator base) {
    return new MaxWindowAccumulator(Math.max(_max, other._max));
  }

  /** @return new accumulator that is the sum of this accumulator, (left - leftbase), and (right - rightbase). */
  @Override
  public MaxWindowAccumulator merge(MaxWindowAccumulator left, MaxWindowAccumulator leftbase,
      MaxWindowAccumulator right, MaxWindowAccumulator rightbase) {
    return new MaxWindowAccumulator(Math.max(_max, Math.max(left._max, right._max)));
  }

  @Override
  public double getFinalValue() {
    return _max;
  }

  @Override
  public int compareTo(MaxWindowAccumulator other) {
    return Double.compare(_max, other._max);
  }

  @Override
  public String toString() {
    return String.valueOf(_max);
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
    byteBuffer.putDouble(_max);
    return byteBuffer.array();
  }

  @Nonnull
  public static MaxWindowAccumulator fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static MaxWindowAccumulator fromByteBuffer(ByteBuffer byteBuffer) {
    return new MaxWindowAccumulator(byteBuffer.getDouble());
  }
}
