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


/** WindowAccumulator object corresponding to the COUNT aggregation function used in window function specification. */
public class CountWindowAccumulator implements WindowAccumulator<CountWindowAccumulator> {
  private long _count;

  /** Public constructor used by WindowAccumulatorFactory. */
  public CountWindowAccumulator() {
    _count = 0;
  }

  /** Constructor for internal use and testing. */
  @VisibleForTesting
  CountWindowAccumulator(long count) {
    _count = count;
  }

  /** Add a raw column value into this accumulator for final computation of window function value. */
  @Override
  public void accumulate(Object value) {
    Preconditions.checkState(value instanceof Number,
        "Value of type " + value.getClass().getName() + " cannot be accumulated by " + getClass().getName());
    _count += 1;
  }

  /** Apply values from input object into this accumulator. Unlike merge functions, no new accumulator is created. */
  @Override
  public void apply(Object value) {
    if (value instanceof Integer) {
      _count += ((Integer) value).doubleValue();
    } else if (value instanceof Long) {
      _count += ((Long) value).longValue();
    } else if (value instanceof Double) {
      _count += ((Double) value).doubleValue();
    } else if (value instanceof CountWindowAccumulator) {
      _count += ((CountWindowAccumulator) value)._count;
    } else {
      throw new IllegalStateException(
          "Value of type " + value.getClass().getName() + " cannot be applied to " + getClass().getName());
    }
  }

  /** @return new accumulator that is the count of this accumulator and the input accumulator. */
  @Override
  public CountWindowAccumulator add(CountWindowAccumulator other) {
    return new CountWindowAccumulator(_count + other._count);
  }

  /** @return new accumulator that is the count of this accumulator and (current - base). */
  @Override
  public CountWindowAccumulator merge(CountWindowAccumulator other, CountWindowAccumulator base) {
    return new CountWindowAccumulator(_count + other._count - base._count);
  }

  /** @return new accumulator that is the count of this accumulator, (left - leftbase), and (right - rightbase). */
  @Override
  public CountWindowAccumulator merge(CountWindowAccumulator left, CountWindowAccumulator leftbase,
      CountWindowAccumulator right, CountWindowAccumulator rightbase) {
    return new CountWindowAccumulator(_count + left._count - leftbase._count + right._count - rightbase._count);
  }

  @Override
  public double getFinalValue() {
    return _count;
  }

  @Override
  public int compareTo(CountWindowAccumulator other) {
    return Double.compare(_count, other._count);
  }

  @Override
  public String toString() {
    return String.valueOf(_count);
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
    byteBuffer.putLong(_count);
    return byteBuffer.array();
  }

  @Nonnull
  public static CountWindowAccumulator fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static CountWindowAccumulator fromByteBuffer(ByteBuffer byteBuffer) {
    return new CountWindowAccumulator(byteBuffer.getLong());
  }
}
