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
package com.facebook.presto.storage;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

public class LocalTempStorageConfig
{
    private String tempStoragePath;
    private double maxUsedSpaceThreshold = 1.0;

    @NotNull
    public String getTempStoragePath()
    {
        return tempStoragePath;
    }

    @Config("temp-storage.path")
    @ConfigDescription("Comma-separated list of paths for local temporary storage")
    public LocalTempStorageConfig setTempStoragePath(String tempStoragePath)
    {
        this.tempStoragePath = tempStoragePath;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMaxUsedSpaceThreshold()
    {
        return maxUsedSpaceThreshold;
    }

    @Config("temp-storage.max-used-space-threshold")
    @ConfigDescription("Maximum disk space usage threshold (0.0 to 1.0) before writes are blocked")
    public LocalTempStorageConfig setMaxUsedSpaceThreshold(double maxUsedSpaceThreshold)
    {
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
        return this;
    }
}
