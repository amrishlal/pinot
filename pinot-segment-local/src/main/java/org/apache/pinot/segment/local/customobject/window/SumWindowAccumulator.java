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


/** WindowAccumulator object corresponding to the SUM aggregation function used in window function specification. */
public class SumWindowAccumulator implements WindowAccumulator<SumWindowAccumulator> {
  private double _sum;

  /** Public constructor used by WindowAccumulatorFactory. */
  public SumWindowAccumulator() {
    _sum = 0;
  }

  /** Constructor for internal use and testing. */
  @VisibleForTesting
  SumWindowAccumulator(double sum) {
    _sum = sum;
  }

  /** Add a raw column value into this accumulator for final computation of window function value. */
  @Override
  public void accumulate(Object value) {
    Preconditions.checkState(value instanceof Number,
        "Value of type " + value.getClass().getName() + " cannot be accumulated by " + getClass().getName());
    _sum += ((Number) value).doubleValue();
  }

  /** Apply values from input object into this accumulator. Unlike merge functions, no new accumulator is created. */
  @Override
  public void apply(Object value) {
    if (value instanceof Double) {
      _sum += ((Double) value).doubleValue();
    } else if (value instanceof SumWindowAccumulator) {
      _sum += ((SumWindowAccumulator) value)._sum;
    } else {
      throw new IllegalStateException(
          "Value of type " + value.getClass().getName() + " cannot be applied to " + getClass().getName());
    }
  }

  /** @return new accumulator that is the sum of this accumulator and the input accumulator. */
  @Override
  public SumWindowAccumulator add(SumWindowAccumulator other) {
    return new SumWindowAccumulator(_sum + other._sum);
  }

  /** @return new accumulator that is the sum of this accumulator and (current - base). */
  @Override
  public SumWindowAccumulator merge(SumWindowAccumulator other, SumWindowAccumulator base) {
    return new SumWindowAccumulator(_sum + other._sum - base._sum);
  }

  /** @return new accumulator that is the sum of this accumulator, (left - leftbase), and (right - rightbase). */
  @Override
  public SumWindowAccumulator merge(SumWindowAccumulator left, SumWindowAccumulator leftbase,
      SumWindowAccumulator right, SumWindowAccumulator rightbase) {
    return new SumWindowAccumulator(_sum + left._sum - leftbase._sum + right._sum - rightbase._sum);
  }

  @Override
  public double getFinalValue() {
    return _sum;
  }

  @Override
  public int compareTo(SumWindowAccumulator other) {
    return Double.compare(_sum, other._sum);
  }

  @Override
  public String toString() {
    return String.valueOf(_sum);
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
    byteBuffer.putDouble(_sum);
    return byteBuffer.array();
  }

  @Nonnull
  public static SumWindowAccumulator fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static SumWindowAccumulator fromByteBuffer(ByteBuffer byteBuffer) {
    return new SumWindowAccumulator(byteBuffer.getDouble());
  }
}
