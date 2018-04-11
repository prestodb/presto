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

import com.facebook.presto.spi.function.ScalarOperator;
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

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NEGATION;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;

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
        long result = 0;
        result += BigintOperators.add(leftOperand0, rightOperand0);
        result += BigintOperators.add(leftOperand1, rightOperand0);
        result += BigintOperators.add(leftOperand2, rightOperand0);
        result += BigintOperators.add(leftOperand3, rightOperand0);
        result += BigintOperators.add(leftOperand4, rightOperand0);
        result += BigintOperators.add(leftOperand0, rightOperand1);
        result += BigintOperators.add(leftOperand1, rightOperand1);
        result += BigintOperators.add(leftOperand2, rightOperand1);
        result += BigintOperators.add(leftOperand3, rightOperand1);
        result += BigintOperators.add(leftOperand4, rightOperand1);
        result += BigintOperators.add(leftOperand0, rightOperand2);
        result += BigintOperators.add(leftOperand1, rightOperand2);
        result += BigintOperators.add(leftOperand2, rightOperand2);
        result += BigintOperators.add(leftOperand3, rightOperand2);
        result += BigintOperators.add(leftOperand4, rightOperand2);
        result += BigintOperators.add(leftOperand0, rightOperand3);
        result += BigintOperators.add(leftOperand1, rightOperand3);
        result += BigintOperators.add(leftOperand2, rightOperand3);
        result += BigintOperators.add(leftOperand3, rightOperand3);
        result += BigintOperators.add(leftOperand4, rightOperand3);
        result += BigintOperators.add(leftOperand0, rightOperand4);
        result += BigintOperators.add(leftOperand1, rightOperand4);
        result += BigintOperators.add(leftOperand2, rightOperand4);
        result += BigintOperators.add(leftOperand3, rightOperand4);
        result += BigintOperators.add(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object overflowChecksSubtract()
    {
        long result = 0;
        result += BigintOperators.subtract(leftOperand0, rightOperand0);
        result += BigintOperators.subtract(leftOperand1, rightOperand0);
        result += BigintOperators.subtract(leftOperand2, rightOperand0);
        result += BigintOperators.subtract(leftOperand3, rightOperand0);
        result += BigintOperators.subtract(leftOperand4, rightOperand0);
        result += BigintOperators.subtract(leftOperand0, rightOperand1);
        result += BigintOperators.subtract(leftOperand1, rightOperand1);
        result += BigintOperators.subtract(leftOperand2, rightOperand1);
        result += BigintOperators.subtract(leftOperand3, rightOperand1);
        result += BigintOperators.subtract(leftOperand4, rightOperand1);
        result += BigintOperators.subtract(leftOperand0, rightOperand2);
        result += BigintOperators.subtract(leftOperand1, rightOperand2);
        result += BigintOperators.subtract(leftOperand2, rightOperand2);
        result += BigintOperators.subtract(leftOperand3, rightOperand2);
        result += BigintOperators.subtract(leftOperand4, rightOperand2);
        result += BigintOperators.subtract(leftOperand0, rightOperand3);
        result += BigintOperators.subtract(leftOperand1, rightOperand3);
        result += BigintOperators.subtract(leftOperand2, rightOperand3);
        result += BigintOperators.subtract(leftOperand3, rightOperand3);
        result += BigintOperators.subtract(leftOperand4, rightOperand3);
        result += BigintOperators.subtract(leftOperand0, rightOperand4);
        result += BigintOperators.subtract(leftOperand1, rightOperand4);
        result += BigintOperators.subtract(leftOperand2, rightOperand4);
        result += BigintOperators.subtract(leftOperand3, rightOperand4);
        result += BigintOperators.subtract(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object overflowChecksMultiply()
    {
        long result = 0;
        result += BigintOperators.multiply(leftOperand0, rightOperand0);
        result += BigintOperators.multiply(leftOperand1, rightOperand0);
        result += BigintOperators.multiply(leftOperand2, rightOperand0);
        result += BigintOperators.multiply(leftOperand3, rightOperand0);
        result += BigintOperators.multiply(leftOperand4, rightOperand0);
        result += BigintOperators.multiply(leftOperand0, rightOperand1);
        result += BigintOperators.multiply(leftOperand1, rightOperand1);
        result += BigintOperators.multiply(leftOperand2, rightOperand1);
        result += BigintOperators.multiply(leftOperand3, rightOperand1);
        result += BigintOperators.multiply(leftOperand4, rightOperand1);
        result += BigintOperators.multiply(leftOperand0, rightOperand2);
        result += BigintOperators.multiply(leftOperand1, rightOperand2);
        result += BigintOperators.multiply(leftOperand2, rightOperand2);
        result += BigintOperators.multiply(leftOperand3, rightOperand2);
        result += BigintOperators.multiply(leftOperand4, rightOperand2);
        result += BigintOperators.multiply(leftOperand0, rightOperand3);
        result += BigintOperators.multiply(leftOperand1, rightOperand3);
        result += BigintOperators.multiply(leftOperand2, rightOperand3);
        result += BigintOperators.multiply(leftOperand3, rightOperand3);
        result += BigintOperators.multiply(leftOperand4, rightOperand3);
        result += BigintOperators.multiply(leftOperand0, rightOperand4);
        result += BigintOperators.multiply(leftOperand1, rightOperand4);
        result += BigintOperators.multiply(leftOperand2, rightOperand4);
        result += BigintOperators.multiply(leftOperand3, rightOperand4);
        result += BigintOperators.multiply(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object overflowChecksDivide()
    {
        long result = 0;
        result += BigintOperators.divide(leftOperand0, rightOperand0);
        result += BigintOperators.divide(leftOperand1, rightOperand0);
        result += BigintOperators.divide(leftOperand2, rightOperand0);
        result += BigintOperators.divide(leftOperand3, rightOperand0);
        result += BigintOperators.divide(leftOperand4, rightOperand0);
        result += BigintOperators.divide(leftOperand0, rightOperand1);
        result += BigintOperators.divide(leftOperand1, rightOperand1);
        result += BigintOperators.divide(leftOperand2, rightOperand1);
        result += BigintOperators.divide(leftOperand3, rightOperand1);
        result += BigintOperators.divide(leftOperand4, rightOperand1);
        result += BigintOperators.divide(leftOperand0, rightOperand2);
        result += BigintOperators.divide(leftOperand1, rightOperand2);
        result += BigintOperators.divide(leftOperand2, rightOperand2);
        result += BigintOperators.divide(leftOperand3, rightOperand2);
        result += BigintOperators.divide(leftOperand4, rightOperand2);
        result += BigintOperators.divide(leftOperand0, rightOperand3);
        result += BigintOperators.divide(leftOperand1, rightOperand3);
        result += BigintOperators.divide(leftOperand2, rightOperand3);
        result += BigintOperators.divide(leftOperand3, rightOperand3);
        result += BigintOperators.divide(leftOperand4, rightOperand3);
        result += BigintOperators.divide(leftOperand0, rightOperand4);
        result += BigintOperators.divide(leftOperand1, rightOperand4);
        result += BigintOperators.divide(leftOperand2, rightOperand4);
        result += BigintOperators.divide(leftOperand3, rightOperand4);
        result += BigintOperators.divide(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object overflowChecksNegate()
    {
        long result = 0;
        result += BigintOperators.negate(leftOperand0);
        result += BigintOperators.negate(leftOperand1);
        result += BigintOperators.negate(leftOperand2);
        result += BigintOperators.negate(leftOperand3);
        result += BigintOperators.negate(leftOperand4);
        result += BigintOperators.negate(rightOperand0);
        result += BigintOperators.negate(rightOperand1);
        result += BigintOperators.negate(rightOperand2);
        result += BigintOperators.negate(rightOperand3);
        result += BigintOperators.negate(rightOperand4);
        return result;
    }

    @Benchmark
    public Object baseLineAdd()
    {
        long result = 0;
        result += addBaseline(leftOperand0, rightOperand0);
        result += addBaseline(leftOperand1, rightOperand0);
        result += addBaseline(leftOperand2, rightOperand0);
        result += addBaseline(leftOperand3, rightOperand0);
        result += addBaseline(leftOperand4, rightOperand0);
        result += addBaseline(leftOperand0, rightOperand1);
        result += addBaseline(leftOperand1, rightOperand1);
        result += addBaseline(leftOperand2, rightOperand1);
        result += addBaseline(leftOperand3, rightOperand1);
        result += addBaseline(leftOperand4, rightOperand1);
        result += addBaseline(leftOperand0, rightOperand2);
        result += addBaseline(leftOperand1, rightOperand2);
        result += addBaseline(leftOperand2, rightOperand2);
        result += addBaseline(leftOperand3, rightOperand2);
        result += addBaseline(leftOperand4, rightOperand2);
        result += addBaseline(leftOperand0, rightOperand3);
        result += addBaseline(leftOperand1, rightOperand3);
        result += addBaseline(leftOperand2, rightOperand3);
        result += addBaseline(leftOperand3, rightOperand3);
        result += addBaseline(leftOperand4, rightOperand3);
        result += addBaseline(leftOperand0, rightOperand4);
        result += addBaseline(leftOperand1, rightOperand4);
        result += addBaseline(leftOperand2, rightOperand4);
        result += addBaseline(leftOperand3, rightOperand4);
        result += addBaseline(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object baseLineSubtract()
    {
        long result = 0;
        result += subtractBaseline(leftOperand0, rightOperand0);
        result += subtractBaseline(leftOperand1, rightOperand0);
        result += subtractBaseline(leftOperand2, rightOperand0);
        result += subtractBaseline(leftOperand3, rightOperand0);
        result += subtractBaseline(leftOperand4, rightOperand0);
        result += subtractBaseline(leftOperand0, rightOperand1);
        result += subtractBaseline(leftOperand1, rightOperand1);
        result += subtractBaseline(leftOperand2, rightOperand1);
        result += subtractBaseline(leftOperand3, rightOperand1);
        result += subtractBaseline(leftOperand4, rightOperand1);
        result += subtractBaseline(leftOperand0, rightOperand2);
        result += subtractBaseline(leftOperand1, rightOperand2);
        result += subtractBaseline(leftOperand2, rightOperand2);
        result += subtractBaseline(leftOperand3, rightOperand2);
        result += subtractBaseline(leftOperand4, rightOperand2);
        result += subtractBaseline(leftOperand0, rightOperand3);
        result += subtractBaseline(leftOperand1, rightOperand3);
        result += subtractBaseline(leftOperand2, rightOperand3);
        result += subtractBaseline(leftOperand3, rightOperand3);
        result += subtractBaseline(leftOperand4, rightOperand3);
        result += subtractBaseline(leftOperand0, rightOperand4);
        result += subtractBaseline(leftOperand1, rightOperand4);
        result += subtractBaseline(leftOperand2, rightOperand4);
        result += subtractBaseline(leftOperand3, rightOperand4);
        result += subtractBaseline(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object baseLineMultiply()
    {
        long result = 0;
        result += multiplyBaseline(leftOperand0, rightOperand0);
        result += multiplyBaseline(leftOperand1, rightOperand0);
        result += multiplyBaseline(leftOperand2, rightOperand0);
        result += multiplyBaseline(leftOperand3, rightOperand0);
        result += multiplyBaseline(leftOperand4, rightOperand0);
        result += multiplyBaseline(leftOperand0, rightOperand1);
        result += multiplyBaseline(leftOperand1, rightOperand1);
        result += multiplyBaseline(leftOperand2, rightOperand1);
        result += multiplyBaseline(leftOperand3, rightOperand1);
        result += multiplyBaseline(leftOperand4, rightOperand1);
        result += multiplyBaseline(leftOperand0, rightOperand2);
        result += multiplyBaseline(leftOperand1, rightOperand2);
        result += multiplyBaseline(leftOperand2, rightOperand2);
        result += multiplyBaseline(leftOperand3, rightOperand2);
        result += multiplyBaseline(leftOperand4, rightOperand2);
        result += multiplyBaseline(leftOperand0, rightOperand3);
        result += multiplyBaseline(leftOperand1, rightOperand3);
        result += multiplyBaseline(leftOperand2, rightOperand3);
        result += multiplyBaseline(leftOperand3, rightOperand3);
        result += multiplyBaseline(leftOperand4, rightOperand3);
        result += multiplyBaseline(leftOperand0, rightOperand4);
        result += multiplyBaseline(leftOperand1, rightOperand4);
        result += multiplyBaseline(leftOperand2, rightOperand4);
        result += multiplyBaseline(leftOperand3, rightOperand4);
        result += multiplyBaseline(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object baseLineDivide()
    {
        long result = 0;
        result += divideBaseline(leftOperand0, rightOperand0);
        result += divideBaseline(leftOperand1, rightOperand0);
        result += divideBaseline(leftOperand2, rightOperand0);
        result += divideBaseline(leftOperand3, rightOperand0);
        result += divideBaseline(leftOperand4, rightOperand0);
        result += divideBaseline(leftOperand0, rightOperand1);
        result += divideBaseline(leftOperand1, rightOperand1);
        result += divideBaseline(leftOperand2, rightOperand1);
        result += divideBaseline(leftOperand3, rightOperand1);
        result += divideBaseline(leftOperand4, rightOperand1);
        result += divideBaseline(leftOperand0, rightOperand2);
        result += divideBaseline(leftOperand1, rightOperand2);
        result += divideBaseline(leftOperand2, rightOperand2);
        result += divideBaseline(leftOperand3, rightOperand2);
        result += divideBaseline(leftOperand4, rightOperand2);
        result += divideBaseline(leftOperand0, rightOperand3);
        result += divideBaseline(leftOperand1, rightOperand3);
        result += divideBaseline(leftOperand2, rightOperand3);
        result += divideBaseline(leftOperand3, rightOperand3);
        result += divideBaseline(leftOperand4, rightOperand3);
        result += divideBaseline(leftOperand0, rightOperand4);
        result += divideBaseline(leftOperand1, rightOperand4);
        result += divideBaseline(leftOperand2, rightOperand4);
        result += divideBaseline(leftOperand3, rightOperand4);
        result += divideBaseline(leftOperand4, rightOperand4);
        return result;
    }

    @Benchmark
    public Object baseLineNegate()
    {
        long result = 0;
        result += negateBaseLine(leftOperand0);
        result += negateBaseLine(leftOperand1);
        result += negateBaseLine(leftOperand2);
        result += negateBaseLine(leftOperand3);
        result += negateBaseLine(leftOperand4);
        result += negateBaseLine(rightOperand0);
        result += negateBaseLine(rightOperand1);
        result += negateBaseLine(rightOperand2);
        result += negateBaseLine(rightOperand3);
        result += negateBaseLine(rightOperand4);
        return result;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    private static long addBaseline(@SqlType(StandardTypes.BIGINT) long first, @SqlType(StandardTypes.BIGINT) long second)
    {
        return first + second;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    private static long subtractBaseline(@SqlType(StandardTypes.BIGINT) long first, @SqlType(StandardTypes.BIGINT) long second)
    {
        return first - second;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.BIGINT)
    private static long multiplyBaseline(@SqlType(StandardTypes.BIGINT) long first, @SqlType(StandardTypes.BIGINT) long second)
    {
        return first * second;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.BIGINT)
    private static long divideBaseline(@SqlType(StandardTypes.BIGINT) long first, @SqlType(StandardTypes.BIGINT) long second)
    {
        return first / second;
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.BIGINT)
    private static long negateBaseLine(@SqlType(StandardTypes.BIGINT) long x)
    {
        return -x;
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
