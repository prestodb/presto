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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.orc.stream.DataOutput;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;

import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PageFileFooterOutput
        implements DataOutput
{
    private final List<Long> stripeOffsets;

    public PageFileFooterOutput(List<Long> stripeOffsets)
    {
        this.stripeOffsets = ImmutableList.copyOf(requireNonNull(stripeOffsets, "stripeOffsets is null"));
    }

    @Override
    public long size()
    {
        return SIZE_OF_LONG * stripeOffsets.size() + SIZE_OF_INT;
    }

    @Override
    public void writeData(SliceOutput sliceOutput)
    {
        for (long offset : stripeOffsets) {
            sliceOutput.writeLong(offset);
        }
        sliceOutput.writeInt(toIntExact(size()));
    }
}
