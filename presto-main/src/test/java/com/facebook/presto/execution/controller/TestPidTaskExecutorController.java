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
package com.facebook.presto.execution.controller;

import com.facebook.presto.execution.controller.PidTaskExecutorController.PidController;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

public class TestPidTaskExecutorController
{
    @Test
    public void testLinearFunctions()
    {
        double targetInput = 37.37;
        Function<Double, Double> function = createLinearFunction(23.23, 123.0);
        double targetOutput = function.apply(targetInput);

        PidController controller = new PidController(-10, 100, targetOutput, 0.0001, 0.0001, 0.00005);
        doTestFunction(controller, targetInput, function);

        // test different linear function
        targetInput = 99.99;
        function = createLinearFunction(3.81, -123.0);
        targetOutput = function.apply(targetInput);

        controller = new PidController(0, 120, targetOutput, 0.0005, 0.0005, 0.00005);
        doTestFunction(controller, targetInput, function);
    }

    @Test
    public void testLinearCombinedFunctions()
    {
        double targetInput = 99.99;
        Function<Double, Double> function = createLinearFunction(3.81, -123.0);
        double targetOutput = function.apply(targetInput);

        PidController controller = new PidController(0, 100, targetOutput, 0.0002, 0.0002, 0.00000001);
        doTestFunction(controller, targetInput, function);

        // balance condition changed
        function = createLinearFunction(23.23, 123.0);
        targetInput = solveLinearFunction(23.23, 123.0, targetOutput);
        doTestFunction(controller, targetInput, function);
    }

    @Test
    public void testQuadraticFunction()
    {
        double targetInput = 9.5;
        Function<Double, Double> function = createQuadraticFunction(-1, 20, 0);
        double targetOutput = function.apply(targetInput);

        PidController controller = new PidController(5, 30, targetOutput, 0.003, 0.003, 0.00003);
        doTestFunction(controller, targetInput, function);
    }

    private void doTestFunction(PidController controller, double expected, Function<Double, Double> function)
    {
        Random random = new Random(0);
        double input = controller.getInput();
        for (int i = 0; i < 100; i++) {
            double seconds = random.nextDouble() / 5 + 0.9;
            controller.feedValue(function.apply(input), seconds);
            input = controller.getInput();
        }
        assertEquals(input, expected, 0.1);
    }

    @Test
    public void testPidTaskExectorController()
    {
        Random random = new Random(0);
        PidTaskExecutorController controller = new PidTaskExecutorController(0.8, 10, 150, 0.1, 0.1, 0.1);
        int input = controller.getNextRunnerThreads(Optional.empty());
        for (int i = 0; i < 100; i++) {
            input = controller.getNextRunnerThreads(Optional.of(createTaskExectorStat(random, input, Integer.MAX_VALUE)));
        }
        assertEquals(input, 59.02, 3);
    }

    private static TaskExecutorStatistics createTaskExectorStat(Random random, int value, int splits)
    {
        // value < 50, output = 0.012 * value + 0.1, output will be below 0.7
        // 50 <= value < 90, output = -0.00015625 * (90 - value)^2 + 0.95, output will be between 0.7 and 0.95
        // value >= 90, output = 0.93 + random(0.04), output will be between 0.93 and 0.97
        double output = 0.012 * value + 0.1;
        if (50 <= value && value < 90) {
            output = 0.95 - 0.00015625 * (90 - value) * (90 - value);
        }
        else if (value >= 90) {
            output = 0.93 + random.nextDouble() / 25.0;
        }
        double randomSeconds = random.nextDouble() / 5 + 0.9;
        return new TaskExecutorStatistics(
                new Duration(output * randomSeconds * Runtime.getRuntime().availableProcessors(), TimeUnit.SECONDS),
                new Duration(randomSeconds, TimeUnit.SECONDS),
                (int) value,
                splits);
    }

    private static Function<Double, Double> createLinearFunction(double a, double b)
    {
        return x -> a * x + b;
    }

    private static double solveLinearFunction(double a, double b, double output)
    {
        return (output - b) / a;
    }

    private static Function<Double, Double> createQuadraticFunction(double a, double b, double c)
    {
        return x -> a * x * x + b * x + c;
    }
}
