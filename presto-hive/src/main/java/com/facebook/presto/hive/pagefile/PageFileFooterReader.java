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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PageFileFooterReader
{
    private static final int EXPECTED_FOOTER_SIZE = 1024;
    private static final int FOOTER_METADATA_SIZE = SIZE_OF_INT;

    private List<Long> stripeOffsets;
    private long footerOffset;

    public PageFileFooterReader(
            FSDataInputStream inputStream,
            long fileSize)
            throws IOException
    {
        requireNonNull(inputStream, "inputStream is null");
        ImmutableList.Builder<Long> stripeOffsetsBuilder = ImmutableList.builder();

        if (fileSize >= FOOTER_METADATA_SIZE) {
            byte[] buffer = new byte[toIntExact(min(fileSize, EXPECTED_FOOTER_SIZE))];
            inputStream.readFully(fileSize - buffer.length, buffer);
            SliceInput sliceInput = Slices.wrappedBuffer(buffer, buffer.length - FOOTER_METADATA_SIZE, FOOTER_METADATA_SIZE).getInput();
            int footerSize = sliceInput.readInt();

            footerOffset = fileSize - footerSize;
            if (footerOffset < 0) {
                throw new IOException("Malformed PageFile format.");
            }

            if (footerSize > buffer.length) {
                buffer = new byte[footerSize];
                inputStream.readFully(footerOffset, buffer);
            }

            sliceInput = Slices.wrappedBuffer(buffer, buffer.length - footerSize, footerSize - FOOTER_METADATA_SIZE).getInput();

            while (sliceInput.isReadable()) {
                stripeOffsetsBuilder.add(sliceInput.readLong());
            }
        }
        stripeOffsets = stripeOffsetsBuilder.build();
    }

    public List<Long> getStripeOffsets()
    {
        return stripeOffsets;
    }

    public long getFooterOffset()
    {
        return footerOffset;
    }
}
