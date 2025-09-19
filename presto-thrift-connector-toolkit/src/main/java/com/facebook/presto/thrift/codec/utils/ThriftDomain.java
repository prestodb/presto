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
package com.facebook.presto.thrift.codec.utils;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.predicate.Range;

import java.util.List;

@ThriftStruct
public class ThriftDomain
{
    private boolean nullAllowed;
    private List<Range> ranges;

    public ThriftDomain() {}

    @ThriftConstructor
    public ThriftDomain(boolean nullAllowed)
    {
        this.nullAllowed = nullAllowed;
        this.ranges = null;
    }

    @ThriftField(value = 1, name = "nullAllowed")
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    public void setNullAllowed(boolean nullAllowed)
    {
        this.nullAllowed = nullAllowed;
    }

    public List<Range> getRanges()
    {
        return ranges;
    }

    public void setRanges(List<Range> ranges)
    {
        this.ranges = ranges;
    }
}
