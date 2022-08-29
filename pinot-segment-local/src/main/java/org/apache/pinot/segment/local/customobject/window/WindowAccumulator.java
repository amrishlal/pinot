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

import javax.annotation.Nonnull;


/**
 * A set of methods which every WindowAccumulator class must have in order for Window Function query
 * evaluation to work properly. Mapping between aggregation functions and their corresponding
 * WindowAccumulators are maintained in WindowAccumulatorFactory class. Serialization functions
 * corresponding to a WindowAccumulator are defined in ObjectSerDeUtils.
 */
public interface WindowAccumulator<T extends WindowAccumulator> extends Comparable<T> {

  /** Add a raw column value into this accumulator for computation of window function value. */
  void accumulate(Object value);

  /** Modify the value of this accumulator based on the input object. */
  void apply(Object value);

  /** @return a new window accumulator object that is the sum of this accumulator and the input window accumulator. */
  T add(T other);

  /** @return a new window accumulator object that merges this accumulator with diff of "other" and "base". */
  T merge(T other, T base);

  /**
   * @return a new accumulator object that merges this accumulator with diff of "left" and "leftbase" and with diff of
   * "right" and rightbase.
   */
  T merge(T left, T leftbase, T right, T rightbase);

  /** @return final value computed by this accumulator. */
  double getFinalValue();

  /**
   * Creates serialized form of the object. All the WindowAccumulator derived classes must be added to
   * ObjectSerDeUtils class and have the following two public static methods:
   *
   * @Nonnull
   * public static WindowAccumulator fromBytes(byte[] bytes);
   * @Nonnull
   * public static WindowAccumulator fromByteBuffer(ByteBuffer byteBuffer);
   *
   * @return serialized of this object.
   */
  @Nonnull
  public byte[] toBytes();
}
