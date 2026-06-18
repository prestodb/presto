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

@ThriftStruct("UdfResult")
public class ThriftUdfResult
{
    private final ThriftUdfPage result;
    private final ThriftUdfStats udfStats;

    @ThriftConstructor
    public ThriftUdfResult(ThriftUdfPage result, ThriftUdfStats udfStats)
    {
        this.result = result;
        this.udfStats = udfStats;
    }

    @ThriftField(value = 1)
    public ThriftUdfPage getResult()
    {
        return result;
    }

    @ThriftField(value = 2)
    public ThriftUdfStats getUdfStats()
    {
        return udfStats;
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
        ThriftUdfResult other = (ThriftUdfResult) obj;
        return Objects.equals(this.result, other.result) &&
                Objects.equals(this.udfStats, other.udfStats);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, udfStats);
    }
}
