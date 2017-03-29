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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class StaticTaskExecutorController
        implements TaskExectorController
{
    private final int runnerThreads;

    public StaticTaskExecutorController(int maxWorkerThreads)
    {
        checkArgument(maxWorkerThreads > 0, "maxWorkerThreads must be greater than 0");
        this.runnerThreads = maxWorkerThreads;
    }

    @Override
    public int getNextRunnerThreads(Optional<TaskExecutorStatistics> stat)
    {
        return runnerThreads;
    }
}
