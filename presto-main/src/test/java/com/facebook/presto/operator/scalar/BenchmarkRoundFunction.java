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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 30, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 30, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRoundFunction
{
    private double operand0;
    private double operand1;
    private double operand2;
    private double operand3;
    private double operand4;
    private float floatOperand0;
    private float floatOperand1;
    private float floatOperand2;
    private float floatOperand3;
    private float floatOperand4;

    @Param({"0", "1", "2", "3", "4"})
    private int numberOfDecimals = 0;

    @Setup
    public void setup()
    {
        operand0 = 0.5;
        operand1 = 754.1985;
        operand2 = -754.2008;
        operand3 = 0x1.fffffffffffffp-2;
        operand4 = -0x1.fffffffffffffp-2;

        floatOperand0 = 0.5f;
        floatOperand1 = 754.1985f;
        floatOperand2 = -754.2008f;
        floatOperand3 = 0x1.fffffep-2f;
        floatOperand4 = -0x1.fffffep-2f;
    }

    @Benchmark
    public void doubleActual(Blackhole bh)
    {
        bh.consume(MathFunctions.round(operand0, numberOfDecimals));
        bh.consume(MathFunctions.round(operand1, numberOfDecimals));
        bh.consume(MathFunctions.round(operand2, numberOfDecimals));
        bh.consume(MathFunctions.round(operand3, numberOfDecimals));
        bh.consume(MathFunctions.round(operand4, numberOfDecimals));
    }

    @Benchmark
    public void doubleBaseline(Blackhole bh)
    {
        bh.consume(roundBaseline(operand0, numberOfDecimals));
        bh.consume(roundBaseline(operand1, numberOfDecimals));
        bh.consume(roundBaseline(operand2, numberOfDecimals));
        bh.consume(roundBaseline(operand3, numberOfDecimals));
        bh.consume(roundBaseline(operand4, numberOfDecimals));
    }

    @Benchmark
    public void floatActual(Blackhole bh)
    {
        bh.consume(MathFunctions.round(floatOperand0, numberOfDecimals));
        bh.consume(MathFunctions.round(floatOperand1, numberOfDecimals));
        bh.consume(MathFunctions.round(floatOperand2, numberOfDecimals));
        bh.consume(MathFunctions.round(floatOperand3, numberOfDecimals));
        bh.consume(MathFunctions.round(floatOperand4, numberOfDecimals));
    }

    @Benchmark
    public void floatBaseline(Blackhole bh)
    {
        bh.consume(roundBaseline(floatOperand0, numberOfDecimals));
        bh.consume(roundBaseline(floatOperand1, numberOfDecimals));
        bh.consume(roundBaseline(floatOperand2, numberOfDecimals));
        bh.consume(roundBaseline(floatOperand3, numberOfDecimals));
        bh.consume(roundBaseline(floatOperand4, numberOfDecimals));
    }

    @Description("round to given number of decimal places")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double roundBaseline(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.BIGINT) long decimals)
    {
        if (num == 0.0) {
            return 0;
        }
        if (num < 0) {
            return -roundBaseline(-num, decimals);
        }

        double factor = Math.pow(10, decimals);
        return Math.floor(num * factor + 0.5) / factor;
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkRoundFunction.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
