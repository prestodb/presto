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
package com.facebook.presto.thrift.api.udf;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import java.util.Objects;

@ThriftStruct("UdfStats")
public class ThriftUdfStats
{
    private final long totalCpuTimeMs;

    @ThriftConstructor
    public ThriftUdfStats(long totalCpuTimeMs)
    {
        this.totalCpuTimeMs = totalCpuTimeMs;
    }

    @ThriftField(value = 1)
    public long getTotalCpuTimeMs()
    {
        return totalCpuTimeMs;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftUdfStats other = (ThriftUdfStats) obj;
        return Objects.equals(this.totalCpuTimeMs, other.totalCpuTimeMs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(totalCpuTimeMs);
    }
}
