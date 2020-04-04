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
package com.facebook.presto.orc;

import com.facebook.presto.orc.StripeReader.StripeId;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Map;

public class StorageStripeMetadataSource
        implements StripeMetadataSource
{
    @Override
    public Slice getStripeFooterSlice(OrcDataSource orcDataSource, StripeId stripeId, long footerOffset, int footerLength, boolean cacheable)
            throws IOException
    {
        byte[] tailBuffer = new byte[footerLength];
        orcDataSource.readFully(footerOffset, tailBuffer);
        return Slices.wrappedBuffer(tailBuffer);
    }

    @Override
    public Map<StreamId, OrcDataSourceInput> getInputs(OrcDataSource orcDataSource, StripeId stripeId, Map<StreamId, DiskRange> diskRanges, boolean cacheable)
            throws IOException
    {
        //
        // Note: this code does not use the Java 8 stream APIs to avoid any extra object allocation
        //

        // transform ranges to have an absolute offset in file
        ImmutableMap.Builder<StreamId, DiskRange> diskRangesBuilder = ImmutableMap.builder();
        for (Map.Entry<StreamId, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            diskRangesBuilder.put(entry.getKey(), new DiskRange(stripeId.getOffset() + diskRange.getOffset(), diskRange.getLength()));
        }
        diskRanges = diskRangesBuilder.build();

        // read ranges
        return orcDataSource.readFully(diskRanges);
    }
}
