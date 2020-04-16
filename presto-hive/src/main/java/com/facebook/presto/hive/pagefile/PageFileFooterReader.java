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

import static com.facebook.presto.hive.pagefile.PageFileFooterOutput.FOOTER_LENGTH_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PageFileFooterReader
{
    private static final int ESTIMATED_FOOTER_SIZE = 1024;

    private List<Long> stripeOffsets;
    private long footerOffset;

    public PageFileFooterReader(
            FSDataInputStream inputStream,
            long fileSize)
            throws IOException
    {
        checkArgument(fileSize >= FOOTER_LENGTH_IN_BYTES, "Malformed PageFile format, footer length is missing.");
        requireNonNull(inputStream, "inputStream is null");
        ImmutableList.Builder<Long> stripeOffsetsBuilder = ImmutableList.builder();

        byte[] buffer = new byte[toIntExact(min(fileSize, ESTIMATED_FOOTER_SIZE))];
        inputStream.readFully(fileSize - buffer.length, buffer);
        int footerSize = Slices.wrappedBuffer(buffer, buffer.length - FOOTER_LENGTH_IN_BYTES, FOOTER_LENGTH_IN_BYTES).getInt(0);

        footerOffset = fileSize - footerSize;
        if (footerOffset < 0) {
            throw new IOException("Malformed PageFile format, incorrect footer length.");
        }
        else if (footerOffset > 0) {
            if (footerSize > buffer.length) {
                buffer = new byte[footerSize];
                inputStream.readFully(footerOffset, buffer);
            }

            SliceInput sliceInput = Slices.wrappedBuffer(buffer, buffer.length - footerSize, footerSize - FOOTER_LENGTH_IN_BYTES).getInput();

            while (sliceInput.isReadable()) {
                stripeOffsetsBuilder.add(sliceInput.readLong());
            }
        }
        else {
            // empty page file without stripe
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
