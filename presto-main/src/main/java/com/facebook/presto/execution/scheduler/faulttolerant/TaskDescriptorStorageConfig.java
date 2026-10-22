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
package com.facebook.presto.execution.scheduler.faulttolerant;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;

public class TaskDescriptorStorageConfig
{
    private DataSize maxMemory = new DataSize(5, GIGABYTE);

    @NotNull
    @MinDataSize("0B")
    public DataSize getMaxMemory()
    {
        return maxMemory;
    }

    @Config("fault-tolerant-execution.task-descriptor-storage-max-memory")
    @ConfigDescription("Maximum memory that can be used to store task descriptors for fault-tolerant execution")
    public TaskDescriptorStorageConfig setMaxMemory(DataSize maxMemory)
    {
        this.maxMemory = maxMemory;
        return this;
    }
}
