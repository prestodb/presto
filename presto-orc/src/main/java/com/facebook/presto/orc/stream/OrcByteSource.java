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
package com.facebook.presto.orc.stream;

import com.google.common.io.ByteSource;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcByteSource
        extends ByteSource
{
    private final OrcInputStream inputStream;
    private final int compressedBlockOffset;
    private final int decompressedOffset;

    public OrcByteSource(OrcInputStream inputStream, int compressedBlockOffset, int decompressedOffset)
    {
        this.inputStream = checkNotNull(inputStream, "inputStream is null");
        this.compressedBlockOffset = compressedBlockOffset;
        this.decompressedOffset = decompressedOffset;
    }

    @Override
    public OrcInputStream openStream()
            throws IOException
    {
        inputStream.resetStream(compressedBlockOffset, decompressedOffset);
        return inputStream;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("inputStream", inputStream)
                .add("rawOffset", compressedBlockOffset)
                .add("decompressedOffset", decompressedOffset)
                .toString();
    }
}
