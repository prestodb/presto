/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.IntegerType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class TestKleinbergBurstDetectionFunction
        extends AbstractTestFunctions
{
    @Test
    public void testKleinbergBurstDetection()
    {
        // Input with one entry.
        assertInvalidFunction(
                "KLEINBERG_BURST_DETECTION(ARRAY[1.1])",
                INVALID_FUNCTION_ARGUMENT,
                "time series must have at least two values");

        // Regular call, using default optional args.
        assertFunction(
                "KLEINBERG_BURST_DETECTION(ARRAY[100, 100.1, 101, 110, 110.1, 110.2, 110.3, 110.35, 110.40, 150, 304.35])",
                new ArrayType(IntegerType.INTEGER),
                ImmutableList.of(0, 2, 2, 2, 6, 6, 6, 6, 6, 0, 0));

        // Regular call, specifying optional args.
        assertFunction(
                "KLEINBERG_BURST_DETECTION(ARRAY[100, 100.1, 101, 110, 110.1, 110.2, 110.3, 110.35, 110.40, 150, 304.35], 2.0, 1.0)",
                new ArrayType(IntegerType.INTEGER),
                ImmutableList.of(0, 2, 2, 2, 6, 6, 6, 6, 6, 0, 0));

        // Integer as input.
        assertFunction(
                "KLEINBERG_BURST_DETECTION(ARRAY[100, 101, 110, 111, 112, 113, 114, 115, 150, 304])",
                new ArrayType(IntegerType.INTEGER),
                ImmutableList.of(0, 2, 2, 3, 3, 3, 3, 3, 0, 0));

        // Input timeseries not sorted.
        assertInvalidFunction(
                "KLEINBERG_BURST_DETECTION(ARRAY[110.40, 110.35])",
                INVALID_FUNCTION_ARGUMENT,
                "time series must be sorted in ascending order");

        // Float as input.
        assertFunction(
                "KLEINBERG_BURST_DETECTION(" +
                        "ARRAY[REAL '100', " +
                        "REAL '100.1', " +
                        "REAL '101', " +
                        "REAL '110', " +
                        "REAL '110.1', " +
                        "REAL '110.2', " +
                        "REAL '110.3', " +
                        "REAL '110.35', " +
                        "REAL '110.40', " +
                        "REAL '150', " +
                        "REAL '304.35'])",
                new ArrayType(IntegerType.INTEGER),
                ImmutableList.of(0, 2, 2, 2, 6, 6, 6, 6, 6, 0, 0));
    }
}
