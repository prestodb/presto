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

public class CpuInfo
        implements ResourceInfo
{
    private final int cpuTimeLabel;
    private final String cpuTimeRange;

    public CpuInfo(int cpuTimeLabel, String cpuTimeRange)
    {
        this.cpuTimeLabel = cpuTimeLabel;
        this.cpuTimeRange = requireNonNull(cpuTimeRange, "CPU time range is null");
    }

    public int getCpuTimeLabel()
    {
        return cpuTimeLabel;
    }

    public String getCpuTimeRange()
    {
        return cpuTimeRange;
    }
}
