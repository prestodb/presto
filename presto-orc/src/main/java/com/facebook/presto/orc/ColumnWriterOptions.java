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

import com.facebook.presto.orc.metadata.CompressionKind;
import io.airlift.units.DataSize;

import java.util.OptionalInt;

import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ColumnWriterOptions
{
    private final CompressionKind compressionKind;
    private final OptionalInt compressionLevel;
    private final int compressionMaxBufferSize;

    public ColumnWriterOptions(CompressionKind compressionKind, OptionalInt compressionLevel, DataSize compressionMaxBufferSize)
    {
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        this.compressionLevel = requireNonNull(compressionLevel, "compressionLevel is null");
        requireNonNull(compressionMaxBufferSize, "compressionMaxBufferSize is null");
        this.compressionMaxBufferSize = toIntExact(compressionMaxBufferSize.toBytes());
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public OptionalInt getCompressionLevel()
    {
        return compressionLevel;
    }

    public int getCompressionMaxBufferSize()
    {
        return compressionMaxBufferSize;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private CompressionKind compressionKind;
        private OptionalInt compressionLevel = OptionalInt.empty();
        private DataSize compressionMaxBufferSize = DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;

        private Builder() {}

        public Builder setCompressionKind(CompressionKind compressionKind)
        {
            this.compressionKind = compressionKind;
            return this;
        }

        public Builder setCompressionLevel(OptionalInt compressionLevel)
        {
            this.compressionLevel = compressionLevel;
            return this;
        }

        public Builder setCompressionMaxBufferSize(DataSize compressionMaxBufferSize)
        {
            this.compressionMaxBufferSize = compressionMaxBufferSize;
            return this;
        }

        public ColumnWriterOptions build()
        {
            return new ColumnWriterOptions(compressionKind, compressionLevel, compressionMaxBufferSize);
        }
    }
}
