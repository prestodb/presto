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
package com.facebook.presto.hive.orc.stream;

import com.facebook.presto.hive.orc.metadata.CompressionKind;
import com.google.common.base.Objects;
import com.google.common.io.ByteSource;
import io.airlift.slice.Slice;

import java.io.IOException;

import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.skipFully;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcByteSource
        extends ByteSource
{
    private final Slice compressedSlice;
    private final CompressionKind compressionKind;
    private final int bufferSize;
    private final int initialOffset;

    public OrcByteSource(Slice slice, CompressionKind compressionKind, int bufferSize, int initialOffset)
    {
        this.compressedSlice = checkNotNull(slice, "slice is null");
        this.compressionKind = checkNotNull(compressionKind, "compressionKind is null");
        this.bufferSize = bufferSize;
        this.initialOffset = initialOffset;
    }

    @Override
    public OrcInputStream openStream()
            throws IOException
    {
        OrcInputStream inputStream = new OrcInputStream(compressedSlice.getInput(), compressionKind, bufferSize);

        if (initialOffset > 0) {
            skipFully(inputStream, initialOffset);
        }

        return inputStream;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("compressedSlice", compressedSlice)
                .add("compressionKind", compressionKind)
                .add("bufferSize", bufferSize)
                .add("initialOffset", initialOffset)
                .toString();
    }
}
