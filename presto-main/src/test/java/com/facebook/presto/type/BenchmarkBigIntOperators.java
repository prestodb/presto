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
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 100, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 100, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkBigIntOperators
{
    private long[] leftOperands;
    private long[] rightOperands;

    @Setup
    public void setup()
    {
        leftOperands = new long[] { 1, 20, 33, 407, 7890 };
        rightOperands = new long[] { 123456, 9003, 809, 67, 5};
    }

    @Benchmark
    public Object overflowChecksAdd()
    {
        return execute((x, y) -> BigintOperators.add(x, y));
    }

    @Benchmark
    public Object overflowChecksSubtract()
    {
        return execute((x, y) -> BigintOperators.subtract(x, y));
    }

    @Benchmark
    public Object overflowChecksMultiply()
    {
        return execute((x, y) -> BigintOperators.multiply(x, y));
    }

    @Benchmark
    public Object overflowChecksDivide()
    {
        return execute((x, y) -> BigintOperators.divide(x, y));
    }

    @Benchmark
    public Object overflowChecksNegate()
    {
        return executeSingleOperand(x -> BigintOperators.negate(x));
    }

    @Benchmark
    public Object baseLineAdd()
    {
        return execute((x, y) -> x + y);
    }

    @Benchmark
    public Object baseLineSubtract()
    {
        return execute((x, y) -> x - y);
    }

    @Benchmark
    public Object baseLineMultiply()
    {
        return execute((x, y) -> x * y);
    }

    @Benchmark
    public Object baseLineDivide()
    {
        return execute((x, y) -> x / y);
    }

    @Benchmark
    public Object baseLineNegate()
    {
        return executeSingleOperand(x -> -x);
    }

    private Object execute(LongBinaryOperator operator)
    {
        long result = 0;
        result += operator.applyAsLong(leftOperands[0], rightOperands[0]);
        result += operator.applyAsLong(leftOperands[1], rightOperands[0]);
        result += operator.applyAsLong(leftOperands[2], rightOperands[0]);
        result += operator.applyAsLong(leftOperands[3], rightOperands[0]);
        result += operator.applyAsLong(leftOperands[4], rightOperands[0]);
        result += operator.applyAsLong(leftOperands[0], rightOperands[1]);
        result += operator.applyAsLong(leftOperands[1], rightOperands[1]);
        result += operator.applyAsLong(leftOperands[2], rightOperands[1]);
        result += operator.applyAsLong(leftOperands[3], rightOperands[1]);
        result += operator.applyAsLong(leftOperands[4], rightOperands[1]);
        result += operator.applyAsLong(leftOperands[0], rightOperands[2]);
        result += operator.applyAsLong(leftOperands[1], rightOperands[2]);
        result += operator.applyAsLong(leftOperands[2], rightOperands[2]);
        result += operator.applyAsLong(leftOperands[3], rightOperands[2]);
        result += operator.applyAsLong(leftOperands[4], rightOperands[2]);
        result += operator.applyAsLong(leftOperands[0], rightOperands[3]);
        result += operator.applyAsLong(leftOperands[1], rightOperands[3]);
        result += operator.applyAsLong(leftOperands[2], rightOperands[3]);
        result += operator.applyAsLong(leftOperands[3], rightOperands[3]);
        result += operator.applyAsLong(leftOperands[4], rightOperands[3]);
        result += operator.applyAsLong(leftOperands[0], rightOperands[4]);
        result += operator.applyAsLong(leftOperands[1], rightOperands[4]);
        result += operator.applyAsLong(leftOperands[2], rightOperands[4]);
        result += operator.applyAsLong(leftOperands[3], rightOperands[4]);
        result += operator.applyAsLong(leftOperands[4], rightOperands[4]);
        return result;
    }

    private Object executeSingleOperand(LongUnaryOperator operator)
    {
        long result = 0;
        result += operator.applyAsLong(leftOperands[0]);
        result += operator.applyAsLong(leftOperands[1]);
        result += operator.applyAsLong(leftOperands[2]);
        result += operator.applyAsLong(leftOperands[3]);
        result += operator.applyAsLong(leftOperands[4]);
        result += operator.applyAsLong(rightOperands[0]);
        result += operator.applyAsLong(rightOperands[1]);
        result += operator.applyAsLong(rightOperands[2]);
        result += operator.applyAsLong(rightOperands[3]);
        result += operator.applyAsLong(rightOperands[4]);
        return result;
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
