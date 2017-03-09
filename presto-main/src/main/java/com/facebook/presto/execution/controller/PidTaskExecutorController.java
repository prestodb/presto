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

import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

// PID here means proportional–integral–derivative controller, for details:
//     http://machinedesign.com/sensors/introduction-pid-control
public class PidTaskExecutorController
        implements TaskExectorController
{
    private final PidController controller;
    private final double target;

    public PidTaskExecutorController(double target, int minValue, int maxValue, double kp, double ki, double kd)
    {
        checkArgument(minValue > 0, "minValue must be greater than 0");
        checkArgument(minValue <= maxValue, "minValue must be less than maxValue");
        checkArgument(target >= 0 && target <= 1, "target must be in range of [0, 1]");

        this.target = target;
        this.controller = new PidController(minValue, maxValue, target, kp, ki, kd);
    }

    @Override
    public int getNewNumRunnerThreads(Optional<TaskExecutorStatistics> statistics)
    {
        requireNonNull(statistics, "statistics is null");

        if (statistics.isPresent()) {
            double cpuUtilization = statistics.get().getCpuUtilization();
            double wallTimeSeoncds = statistics.get().getWallTime().getValue(SECONDS);
            if (statistics.get().getTotalSplits() <= statistics.get().getRunnerThreads()) {
                cpuUtilization = Math.max(target, cpuUtilization);
            }
            controller.feedValue(cpuUtilization, wallTimeSeoncds);
        }
        return (int) controller.getInput();
    }

    @VisibleForTesting
    static class PidController
    {
        private final double minValue;
        private final double maxValue;
        private final double target;
        private final double kp;
        private final double ki;
        private final double kd;

        private double ratio;
        private double integralValue;
        private double lastError;

        PidController(double minValue, double maxValue, double target, double kp, double ki, double kd)
        {
            this.lastError = 0;
            this.target = target;
            this.minValue = minValue;
            this.maxValue = maxValue;

            this.kp = kp;
            this.ki = ki;
            this.kd = kd;
        }

        public void feedValue(double value, double dt)
        {
            double error = target - value;

            ratio = kp * error + ki * (integralValue + error * dt) + kd * (error - lastError) / dt;

            // Do integration if and only if the generated value is within the allowed range
            if (ratio <= 1 && ratio >= 0) {
                integralValue += error * dt;
            }
            lastError = error;
        }

        public double getInput()
        {
            return Math.min(maxValue, Math.max(minValue, minValue + (maxValue - minValue) * ratio));
        }
    }
}
