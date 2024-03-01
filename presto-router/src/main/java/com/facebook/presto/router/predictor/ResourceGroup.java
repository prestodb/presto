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
package com.facebook.presto.router.predictor;

import static java.util.Objects.requireNonNull;

public class ResourceGroup
{
    private final CpuInfo cpuInfo;
    private final MemoryInfo memoryInfo;

    public ResourceGroup(CpuInfo cpuInfo, MemoryInfo memoryInfo)
    {
        this.cpuInfo = requireNonNull(cpuInfo, "CpuInfo is null");
        this.memoryInfo = requireNonNull(memoryInfo, "MemoryInfo is null");
    }

    public CpuInfo getCpuInfo()
    {
        return cpuInfo;
    }

    public MemoryInfo getMemoryInfo()
    {
        return memoryInfo;
    }
}
