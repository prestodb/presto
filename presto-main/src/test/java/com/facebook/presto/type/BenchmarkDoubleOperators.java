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
package com.facebook.presto.type;

import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.floatToRawIntBits;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 30, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 30, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkDoubleOperators
{
    private double maxDouble;
    private double minNegativeDouble;
    private double minPositiveDouble;
    private double zero;
    private double one;
    private double double1;
    private double double2;
    private double double3;
    private double double4;
    private double double5;

    @Setup
    public void setup()
    {
        maxDouble = Double.MAX_VALUE;
        minNegativeDouble = -1 * Double.MAX_VALUE;
        minPositiveDouble = Double.MIN_VALUE;
        zero = 0;
        one = 1;
        double1 = 3e43;
        double2 = -4e43;
        double3 = -4e22;
        double4 = 4e11;
        double5 = 123456789101112.27272d;
    }

    @Benchmark
    public strictfp Object floorCastToFloatStrictFP()
    {
        float result = 0;
        result += DoubleOperators.saturatedFloorCastToFloat(maxDouble);
        result += DoubleOperators.saturatedFloorCastToFloat(minNegativeDouble);
        result += DoubleOperators.saturatedFloorCastToFloat(minPositiveDouble);
        result += DoubleOperators.saturatedFloorCastToFloat(zero);
        result += DoubleOperators.saturatedFloorCastToFloat(one);
        result += DoubleOperators.saturatedFloorCastToFloat(double1);
        result += DoubleOperators.saturatedFloorCastToFloat(double2);
        result += DoubleOperators.saturatedFloorCastToFloat(double3);
        result += DoubleOperators.saturatedFloorCastToFloat(double4);
        result += DoubleOperators.saturatedFloorCastToFloat(double5);
        result += DoubleOperators.saturatedFloorCastToFloat(maxDouble);
        result += DoubleOperators.saturatedFloorCastToFloat(minNegativeDouble);
        result += DoubleOperators.saturatedFloorCastToFloat(minPositiveDouble);
        result += DoubleOperators.saturatedFloorCastToFloat(zero);
        result += DoubleOperators.saturatedFloorCastToFloat(one);
        result += DoubleOperators.saturatedFloorCastToFloat(double1);
        result += DoubleOperators.saturatedFloorCastToFloat(double2);
        result += DoubleOperators.saturatedFloorCastToFloat(double3);
        result += DoubleOperators.saturatedFloorCastToFloat(double4);
        result += DoubleOperators.saturatedFloorCastToFloat(double5);
        return result;
    }

    @Benchmark
    public Object floorCastToFloat()
    {
        float result = 0;
        result += saturatedFloorCastToFloat(maxDouble);
        result += saturatedFloorCastToFloat(minNegativeDouble);
        result += saturatedFloorCastToFloat(minPositiveDouble);
        result += saturatedFloorCastToFloat(zero);
        result += saturatedFloorCastToFloat(one);
        result += saturatedFloorCastToFloat(double1);
        result += saturatedFloorCastToFloat(double2);
        result += saturatedFloorCastToFloat(double3);
        result += saturatedFloorCastToFloat(double4);
        result += saturatedFloorCastToFloat(double5);
        result += saturatedFloorCastToFloat(maxDouble);
        result += saturatedFloorCastToFloat(minNegativeDouble);
        result += saturatedFloorCastToFloat(minPositiveDouble);
        result += saturatedFloorCastToFloat(zero);
        result += saturatedFloorCastToFloat(one);
        result += saturatedFloorCastToFloat(double1);
        result += saturatedFloorCastToFloat(double2);
        result += saturatedFloorCastToFloat(double3);
        result += saturatedFloorCastToFloat(double4);
        result += saturatedFloorCastToFloat(double5);
        return result;
    }

    public static long saturatedFloorCastToFloat(@SqlType(StandardTypes.DOUBLE) double value)
    {
        float result;
        float minFloat = -1.0f * Float.MAX_VALUE;
        if (value <= minFloat) {
            result = minFloat;
        }
        else if (value >= Float.MAX_VALUE) {
            result = Float.MAX_VALUE;
        }
        else {
            result = (float) value;
            if (result > value) {
                result = Math.nextDown(result);
            }
            checkState(result <= value);
        }
        return floatToRawIntBits(result);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDoubleOperators.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
