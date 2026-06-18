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

import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.pagefile.PageFileFooterOutput.FOOTER_LENGTH_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PageFileFooterReader
{
    private static final int ESTIMATED_FOOTER_SIZE = 1024;

    private final List<Long> stripeOffsets;
    private final long footerOffset;
    private final Optional<HiveCompressionCodec> compressionCodec;

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
        HiveCompressionCodec compression;
        if (footerOffset < 0) {
            throw new PrestoException(HIVE_BAD_DATA, "Malformed PageFile format, incorrect footer length.");
        }
        else if (footerOffset > 0) {
            if (footerSize > buffer.length) {
                buffer = new byte[footerSize];
                inputStream.readFully(footerOffset, buffer);
            }

            FixedLengthSliceInput sliceInput = Slices.wrappedBuffer(buffer, buffer.length - footerSize, footerSize - FOOTER_LENGTH_IN_BYTES).getInput();
            long remainingSize = sliceInput.length();
            // read compression
            int compressionStringSize = sliceInput.readInt();
            remainingSize -= SIZE_OF_INT;
            String compressionName = sliceInput.readSlice(compressionStringSize).toStringUtf8();
            remainingSize -= compressionStringSize;

            try {
                compression = HiveCompressionCodec.valueOf(compressionName);
            }
            catch (Exception e) {
                throw new PrestoException(
                        HIVE_BAD_DATA,
                        format("%s is invalid compression method in the footer of %s", compressionName, PAGEFILE.getInputFormat()));
            }

            // read stripeOffsets
            int stripeCount = sliceInput.readInt();
            remainingSize -= SIZE_OF_INT;
            if (remainingSize != SIZE_OF_LONG * stripeCount) {
                throw new PrestoException(HIVE_BAD_DATA, "Malformed PageFile format, incorrect stripe count.");
            }
            for (int i = 0; i < stripeCount; ++i) {
                stripeOffsetsBuilder.add(sliceInput.readLong());
            }
        }
        else {
            // empty page file without stripe
            compression = null;
        }
        compressionCodec = Optional.ofNullable(compression);
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

    public Optional<HiveCompressionCodec> getCompression()
    {
        return compressionCodec;
    }
}
