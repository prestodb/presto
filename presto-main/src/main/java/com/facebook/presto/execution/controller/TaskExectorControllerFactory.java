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

import com.facebook.presto.execution.TaskManagerConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class TaskExectorControllerFactory
{
    private TaskExectorControllerFactory() {}

    public static TaskExectorController createTaskExecutorController(TaskManagerConfig config)
    {
        requireNonNull(config, "config is null");
        checkArgument(config.getMinWorkerThreads() <= config.getMaxWorkerThreads(), "minWorkerThreads should be less than maxWorkerThreads");

        if (config.getMinWorkerThreads() == config.getMaxWorkerThreads()) {
            return new StaticTaskExecutorController(config.getMaxWorkerThreads());
        }

        // choose conservative parameters for PID (kp, ki, kd) controller as default values,
        // these parameters (kp, ki, kd) might be exposed to user in the future
        return new PidTaskExecutorController(
                config.getTargetCpuUtilization(),
                config.getMinWorkerThreads(),
                config.getMaxWorkerThreads(),
                0.1,
                0.1,
                0.01);
    }
}
