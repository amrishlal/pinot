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
import org.apache.pinot.segment.local.customobject.AvgPair;

/** WindowAccumulator object corresponding to the AVG aggregation function used in window function specification. */
public class AvgWindowAccumulator implements WindowAccumulator<AvgWindowAccumulator> {
  private AvgPair _avgPair;

  /** Public constructor used by WindowAccumulatorFactory. */
  public AvgWindowAccumulator() {
    _avgPair = new AvgPair(0, 0);
  }

  /** Constructor for internal use and testing. */
  @VisibleForTesting
  AvgWindowAccumulator(double sum, long count) {
    _avgPair = new AvgPair(sum, count);
  }

  /** Add a raw column value into this accumulator for final computation of window function value. */
  @Override
  public void accumulate(Object value) {
    _avgPair.apply(((Number) value).doubleValue(), 1);
  }

  /** Apply values from input object into this accumulator. Unlike merge functions, no new accumulator is created. */
  @Override
  public void apply(Object value) {
    if (value instanceof AvgPair) {
      _avgPair.apply((AvgPair) value);
    } else if (value instanceof AvgWindowAccumulator) {
      _avgPair.apply(((AvgWindowAccumulator) value)._avgPair);
    } else {
      throw new IllegalStateException(
          "Value of type " + value.getClass().getName() + " cannot be applied to " + getClass().getName());
    }
  }

  /** @return new accumulator that is the sum of this accumulator and the input accumulator. */
  @Override
  public AvgWindowAccumulator add(AvgWindowAccumulator other) {
    return new AvgWindowAccumulator(_avgPair.getSum() + other._avgPair.getSum(),
        _avgPair.getCount() + other._avgPair.getCount());
  }

  /** @return new accumulator that is the sum of this accumulator and (current - base). */
  @Override
  public AvgWindowAccumulator merge(AvgWindowAccumulator other, AvgWindowAccumulator base) {
    return new AvgWindowAccumulator(
        _avgPair.getSum() + other._avgPair.getSum() - base._avgPair.getSum(),
        _avgPair.getCount() + other._avgPair.getCount() - base._avgPair.getCount());
  }

  /** @return new accumulator that is the sum of this accumulator, (left - leftbase), and (right - rightbase). */
  @Override
  public AvgWindowAccumulator merge(AvgWindowAccumulator left, AvgWindowAccumulator leftbase,
      AvgWindowAccumulator right, AvgWindowAccumulator rightbase) {

    return new AvgWindowAccumulator(
        _avgPair.getSum() + left._avgPair.getSum() - leftbase._avgPair.getSum()
            + right._avgPair.getSum() - rightbase._avgPair.getSum(),
        _avgPair.getCount() + left._avgPair.getCount() - leftbase._avgPair.getCount()
            + right._avgPair.getCount() - rightbase._avgPair.getCount());
  }

  @Override
  public double getFinalValue() {
    return _avgPair.getAverage();
  }

  @Override
  public int compareTo(AvgWindowAccumulator other) {
    return _avgPair.compareTo(other._avgPair);
  }

  @Override
  public String toString() {
    return _avgPair.toString();
  }

  @Nonnull
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Long.BYTES);
    byteBuffer.putDouble(_avgPair.getSum());
    byteBuffer.putLong(_avgPair.getCount());
    return byteBuffer.array();
  }

  @Nonnull
  public static AvgWindowAccumulator fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static AvgWindowAccumulator fromByteBuffer(ByteBuffer byteBuffer) {
    return new AvgWindowAccumulator(byteBuffer.getDouble(), byteBuffer.getLong());
  }
}
