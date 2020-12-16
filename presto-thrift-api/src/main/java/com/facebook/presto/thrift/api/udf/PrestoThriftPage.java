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
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;

import java.util.List;
import java.util.Objects;

@ThriftStruct
public class PrestoThriftPage
{
    private final List<PrestoThriftBlock> thriftBlocks;
    private final int positionCount;

    @ThriftConstructor
    public PrestoThriftPage(List<PrestoThriftBlock> thriftBlocks, int positionCount)
    {
        this.thriftBlocks = thriftBlocks;
        this.positionCount = positionCount;
    }

    @ThriftField(1)
    public List<PrestoThriftBlock> getThriftBlocks()
    {
        return thriftBlocks;
    }

    @ThriftField(2)
    public int getPositionCount()
    {
        return positionCount;
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
        PrestoThriftPage other = (PrestoThriftPage) obj;
        return Objects.equals(this.thriftBlocks, other.thriftBlocks) &&
                this.positionCount == other.positionCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(thriftBlocks, positionCount);
    }
}
