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

import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBigIntOperators
{
    private static final long NUMBER_OF_REPETITIONS = 100_000;

    @Benchmark
    public Object overflowChecks(BenchmarkData data)
    {
        switch (data.type) {
            case "add":
                return execute(data, (x, y) -> BigintOperators.add(x, y));
            case "subtract":
                return execute(data, (x, y) -> BigintOperators.subtract(x, y));
            case "multiply":
                return execute(data, (x, y) -> BigintOperators.multiply(x, y));
            case "divide":
                return execute(data, (x, y) -> BigintOperators.divide(x, y));
            case "negate":
                return executeSingleOperand(data, x -> BigintOperators.negate(x));
            default:
                throw new IllegalStateException("Operator not supported");
        }
    }

    @Benchmark
    public Object baseLine(BenchmarkData data)
    {
        switch (data.type) {
            case "add":
                return execute(data, (x, y) -> x + y);
            case "subtract":
                return execute(data, (x, y) -> x - y);
            case "multiply":
                return execute(data, (x, y) -> x * y);
            case "divide":
                return execute(data, (x, y) -> x / y);
            case "negate":
                return executeSingleOperand(data, x -> -x);
            default:
                throw new IllegalStateException("Operator not supported");
        }
    }

    private static Object execute(BenchmarkData data, LongBinaryOperator operator)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (long i = 0; i < NUMBER_OF_REPETITIONS; ++i) {
            for (long left : data.leftOperands) {
                for (long right : data.rightOperands) {
                    builder.add(operator.applyAsLong(left, right));
                }
            }
        }
        return builder.build();
    }

    private static Object executeSingleOperand(BenchmarkData data, LongUnaryOperator operator)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (long i = 0; i < NUMBER_OF_REPETITIONS; ++i) {
            for (long left : data.leftOperands) {
                    builder.add(operator.applyAsLong(left));
            }
        }
        return builder.build();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        public long[] leftOperands = {1, 20, 33, 407, 7890};
        public long[] rightOperands = {123456, 9003, 809, 67, 5};

        @Param({"add", "subtract", "multiply", "divide", "negate"})
        public String type;
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBigIntOperators.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
