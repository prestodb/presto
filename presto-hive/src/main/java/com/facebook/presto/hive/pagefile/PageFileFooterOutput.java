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

import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PageFileFooterOutput
        implements DataOutput
{
    public static final int FOOTER_LENGTH_IN_BYTES = SIZE_OF_INT;

    private final List<Long> stripeOffsets;
    private final Slice compressionSlice;

    public PageFileFooterOutput(List<Long> stripeOffsets, HiveCompressionCodec compressionCodec)
    {
        this.stripeOffsets = ImmutableList.copyOf(requireNonNull(stripeOffsets, "stripeOffsets is null"));
        compressionSlice = utf8Slice(requireNonNull(compressionCodec, "compressionCodec is null").name());
    }

    @Override
    public long size()
    {
        long size = FOOTER_LENGTH_IN_BYTES;
        if (!stripeOffsets.isEmpty()) {
            size += SIZE_OF_INT + compressionSlice.length() +
                    SIZE_OF_INT + SIZE_OF_LONG * stripeOffsets.size();
        }
        return size;
    }

    @Override
    public void writeData(SliceOutput sliceOutput)
    {
        if (!stripeOffsets.isEmpty()) {
            // write compression information
            sliceOutput.writeInt(compressionSlice.length());
            sliceOutput.writeBytes(compressionSlice);

            // write stripe count and offsets
            sliceOutput.writeInt(stripeOffsets.size());
            for (long offset : stripeOffsets) {
                sliceOutput.writeLong(offset);
            }
        }
        // write footer length
        sliceOutput.writeInt(toIntExact(size()));
    }

    public static PageFileFooterOutput createEmptyPageFileFooterOutput()
    {
        return new PageFileFooterOutput(ImmutableList.of(), HiveCompressionCodec.NONE);
    }
}
