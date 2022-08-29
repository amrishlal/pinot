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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/** Test class for {@link AvgWindowAccumulator}. */
public class AvgWindowAccumulatorTest {

  @Test
  public void testAvg() {
    // Check empty constructor
    AvgWindowAccumulator accumulator1 = new AvgWindowAccumulator();
    assertEquals(accumulator1.compareTo(new AvgWindowAccumulator(0d, 0L)), 0);

    // Check value accumulation
    accumulator1.accumulate(Double.valueOf(5.5d));
    accumulator1.accumulate(Integer.valueOf(4));
    accumulator1.accumulate(Long.valueOf(10L));
    assertEquals(accumulator1.compareTo(new AvgWindowAccumulator(19.5d, 3L)), 0);

    // Check add
    AvgWindowAccumulator accumulator2 = new AvgWindowAccumulator(5, 1);
    AvgWindowAccumulator accumulator3 = accumulator1.add(accumulator2);
    assertNotEquals(accumulator1, accumulator3);
    assertEquals(accumulator3.compareTo(new AvgWindowAccumulator(24.5, 4L)), 0);

    // check merge
    AvgWindowAccumulator accumulator4 = accumulator3.merge(accumulator1, accumulator2);
    assertNotEquals(accumulator3, accumulator4);
    assertEquals(accumulator4.compareTo(new AvgWindowAccumulator(39d, 6L)), 0);

    AvgWindowAccumulator accumulator5 = new AvgWindowAccumulator();
    AvgWindowAccumulator accumulator6 = accumulator5.merge(accumulator1, accumulator2, accumulator3, accumulator1);
    assertEquals(accumulator6.compareTo(new AvgWindowAccumulator(19.5, 3)), 0);
    assertEquals(accumulator6.getFinalValue(), 6.5);

    // check serialization and deserialization
    byte[] bytes = accumulator6.toBytes();
    AvgWindowAccumulator accumulator7 = AvgWindowAccumulator.fromBytes(bytes);
    assertEquals(accumulator6.compareTo(accumulator6), 0);
  }
}
