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
@Fork(20)
@Warmup(iterations = 100, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 100, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkBigIntOperators
{
    @Benchmark
    public Object overflowChecks_add()
    {
        return execute((x, y) -> BigintOperators.add(x, y));
    }

    @Benchmark
    public Object overflowChecks_subtract()
    {
        return execute((x, y) -> BigintOperators.subtract(x, y));
    }

    @Benchmark
    public Object overflowChecks_multiply()
    {
        return execute((x, y) -> BigintOperators.multiply(x, y));
    }

    @Benchmark
    public Object overflowChecks_divide()
    {
        return execute((x, y) -> BigintOperators.divide(x, y));
    }

    @Benchmark
    public Object overflowChecks_negate()
    {
        return executeSingleOperand(x -> BigintOperators.negate(x));
    }

    @Benchmark
    public Object baseLine_add()
    {
        return execute((x, y) -> x + y);
    }

    @Benchmark
    public Object baseLine_subtract()
    {
        return execute((x, y) -> x - y);
    }

    @Benchmark
    public Object baseLine_multiply()
    {
        return execute((x, y) -> x * y);
    }

    @Benchmark
    public Object baseLine_divide()
    {
        return execute((x, y) -> x / y);
    }

    @Benchmark
    public Object baseLine_negate()
    {
        return executeSingleOperand(x -> -x);
    }

    private static Object execute(LongBinaryOperator operator)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        builder.add(operator.applyAsLong(1, 123456));
        builder.add(operator.applyAsLong(20, 123456));
        builder.add(operator.applyAsLong(33, 123456));
        builder.add(operator.applyAsLong(407, 123456));
        builder.add(operator.applyAsLong(7890, 123456));
        builder.add(operator.applyAsLong(1, 9003));
        builder.add(operator.applyAsLong(20, 9003));
        builder.add(operator.applyAsLong(33, 9003));
        builder.add(operator.applyAsLong(407, 9003));
        builder.add(operator.applyAsLong(7890, 9003));
        builder.add(operator.applyAsLong(1, 809));
        builder.add(operator.applyAsLong(20, 809));
        builder.add(operator.applyAsLong(33, 809));
        builder.add(operator.applyAsLong(407, 809));
        builder.add(operator.applyAsLong(7890, 809));
        builder.add(operator.applyAsLong(1, 67));
        builder.add(operator.applyAsLong(20, 67));
        builder.add(operator.applyAsLong(33, 67));
        builder.add(operator.applyAsLong(407, 67));
        builder.add(operator.applyAsLong(7890, 67));
        builder.add(operator.applyAsLong(1, 5));
        builder.add(operator.applyAsLong(20, 5));
        builder.add(operator.applyAsLong(33, 5));
        builder.add(operator.applyAsLong(407, 5));
        builder.add(operator.applyAsLong(7890, 5));
        return builder.build();
    }

    private static Object executeSingleOperand(LongUnaryOperator operator)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        builder.add(operator.applyAsLong(1));
        builder.add(operator.applyAsLong(20));
        builder.add(operator.applyAsLong(33));
        builder.add(operator.applyAsLong(407));
        builder.add(operator.applyAsLong(7890));
        builder.add(operator.applyAsLong(123456));
        builder.add(operator.applyAsLong(9003));
        builder.add(operator.applyAsLong(809));
        builder.add(operator.applyAsLong(67));
        builder.add(operator.applyAsLong(5));
        return builder.build();
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
