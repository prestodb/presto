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
    private long leftOperand0;
    private long leftOperand1;
    private long leftOperand2;
    private long leftOperand3;
    private long leftOperand4;

    private long rightOperand0;
    private long rightOperand1;
    private long rightOperand2;
    private long rightOperand3;
    private long rightOperand4;

    @Setup
    public void setup()
    {
        leftOperand0 = 1;
        leftOperand1 = 20;
        leftOperand2 = 33;
        leftOperand3 = 407;
        leftOperand4 = 7890;

        rightOperand0 = 123456;
        rightOperand1 = 9003;
        rightOperand2 = 809;
        rightOperand3 = 67;
        rightOperand4 = 5;
    }

    @Benchmark
    public Object overflowChecksAdd()
    {
        return execute(BigintOperators::add);
    }

    @Benchmark
    public Object overflowChecksSubtract()
    {
        return execute(BigintOperators::subtract);
    }

    @Benchmark
    public Object overflowChecksMultiply()
    {
        return execute(BigintOperators::multiply);
    }

    @Benchmark
    public Object overflowChecksDivide()
    {
        return execute(BigintOperators::divide);
    }

    @Benchmark
    public Object overflowChecksNegate()
    {
        return executeSingleOperand(x -> BigintOperators.negate(x));
    }

    @Benchmark
    public Object baseLineAdd()
    {
        return execute(BenchmarkBigIntOperators::addBaseline);
    }

    @Benchmark
    public Object baseLineSubtract()
    {
        return execute(BenchmarkBigIntOperators::subtractBaseline);
    }

    @Benchmark
    public Object baseLineMultiply()
    {
        return execute(BenchmarkBigIntOperators::multiplyBaseline);
    }

    @Benchmark
    public Object baseLineDivide()
    {
        return execute(BenchmarkBigIntOperators::divideBaseline);
    }

    @Benchmark
    public Object baseLineNegate()
    {
        return executeSingleOperand(BenchmarkBigIntOperators::negateBaseLine);
    }

    private static long addBaseline(long first, long second)
    {
        return first + second;
    }

    private static long subtractBaseline(long first, long second)
    {
        return first - second;
    }

    private static long multiplyBaseline(long first, long second)
    {
        return first * second;
    }

    private static long divideBaseline(long first, long second)
    {
        return first / second;
    }

    private static long negateBaseLine(long x)
    {
        return -x;
    }

    private Object execute(LongBinaryOperator operator)
    {
        long result = 0;
        result += operator.applyAsLong(leftOperand0, rightOperand0);
        result += operator.applyAsLong(leftOperand1, rightOperand0);
        result += operator.applyAsLong(leftOperand2, rightOperand0);
        result += operator.applyAsLong(leftOperand3, rightOperand0);
        result += operator.applyAsLong(leftOperand4, rightOperand0);
        result += operator.applyAsLong(leftOperand0, rightOperand1);
        result += operator.applyAsLong(leftOperand1, rightOperand1);
        result += operator.applyAsLong(leftOperand2, rightOperand1);
        result += operator.applyAsLong(leftOperand3, rightOperand1);
        result += operator.applyAsLong(leftOperand4, rightOperand1);
        result += operator.applyAsLong(leftOperand0, rightOperand2);
        result += operator.applyAsLong(leftOperand1, rightOperand2);
        result += operator.applyAsLong(leftOperand2, rightOperand2);
        result += operator.applyAsLong(leftOperand3, rightOperand2);
        result += operator.applyAsLong(leftOperand4, rightOperand2);
        result += operator.applyAsLong(leftOperand0, rightOperand3);
        result += operator.applyAsLong(leftOperand1, rightOperand3);
        result += operator.applyAsLong(leftOperand2, rightOperand3);
        result += operator.applyAsLong(leftOperand3, rightOperand3);
        result += operator.applyAsLong(leftOperand4, rightOperand3);
        result += operator.applyAsLong(leftOperand0, rightOperand4);
        result += operator.applyAsLong(leftOperand1, rightOperand4);
        result += operator.applyAsLong(leftOperand2, rightOperand4);
        result += operator.applyAsLong(leftOperand3, rightOperand4);
        result += operator.applyAsLong(leftOperand4, rightOperand4);
        return result;
    }

    private Object executeSingleOperand(LongUnaryOperator operator)
    {
        long result = 0;
        result += operator.applyAsLong(leftOperand0);
        result += operator.applyAsLong(leftOperand1);
        result += operator.applyAsLong(leftOperand2);
        result += operator.applyAsLong(leftOperand3);
        result += operator.applyAsLong(leftOperand4);
        result += operator.applyAsLong(rightOperand0);
        result += operator.applyAsLong(rightOperand1);
        result += operator.applyAsLong(rightOperand2);
        result += operator.applyAsLong(rightOperand3);
        result += operator.applyAsLong(rightOperand4);
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
