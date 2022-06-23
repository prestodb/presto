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
package com.facebook.presto.parquet.reader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MockInputStreamTail
{
    public static final int MAX_SUPPORTED_PADDING_BYTES = 64;
    private static final int MAXIMUM_READ_LENGTH = Integer.MAX_VALUE - (MAX_SUPPORTED_PADDING_BYTES + 1);

    private final Slice tailSlice;
    private final long fileSize;

    private MockInputStreamTail(long fileSize, Slice tailSlice)
    {
        this.tailSlice = requireNonNull(tailSlice, "tailSlice is null");
        this.fileSize = fileSize;
        checkArgument(fileSize >= 0, "fileSize is negative: %s", fileSize);
        checkArgument(tailSlice.length() <= fileSize, "length (%s) is greater than fileSize (%s)", tailSlice.length(), fileSize);
    }

    public static MockInputStreamTail readTail(String path, long paddedFileSize, FSDataInputStream inputStream, int length)
            throws IOException
    {
        checkArgument(length >= 0, "length is negative: %s", length);
        checkArgument(length <= MAXIMUM_READ_LENGTH, "length (%s) exceeds maximum (%s)", length, MAXIMUM_READ_LENGTH);
        long readSize = min(paddedFileSize, (length + MAX_SUPPORTED_PADDING_BYTES));
        long position = paddedFileSize - readSize;
        // Actual read will be 1 byte larger to ensure we encounter an EOF where expected
        byte[] buffer = new byte[toIntExact(readSize + 1)];
        int bytesRead = 0;
        long startPos = inputStream.getPos();
        try {
            inputStream.seek(position);
            while (bytesRead < buffer.length) {
                int n = inputStream.read(buffer, bytesRead, buffer.length - bytesRead);
                if (n < 0) {
                    break;
                }
                bytesRead += n;
            }
        }
        finally {
            inputStream.seek(startPos);
        }
        if (bytesRead > readSize) {
            throw rejectInvalidFileSize(path, paddedFileSize);
        }
        return new MockInputStreamTail(position + bytesRead, Slices.wrappedBuffer(buffer, max(0, bytesRead - length), min(bytesRead, length)));
    }

    public static long readTailForFileSize(String path, long paddedFileSize, FSDataInputStream inputStream)
            throws IOException
    {
        long position = max(paddedFileSize - MAX_SUPPORTED_PADDING_BYTES, 0);
        long maxEOFAt = paddedFileSize + 1;
        long startPos = inputStream.getPos();
        try {
            inputStream.seek(position);
            int c;
            while (position < maxEOFAt) {
                c = inputStream.read();
                if (c < 0) {
                    return position;
                }
                position++;
            }
            throw rejectInvalidFileSize(path, paddedFileSize);
        }
        finally {
            inputStream.seek(startPos);
        }
    }

    private static IOException rejectInvalidFileSize(String path, long reportedSize)
            throws IOException
    {
        throw new IOException(format("Incorrect file size (%s) for file (end of stream not reached): %s", reportedSize, path));
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public Slice getTailSlice()
    {
        return tailSlice;
    }
}
