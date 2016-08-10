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
package com.facebook.presto.orc.compression;

import com.facebook.presto.orc.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.orc.metadata.CompressionKind;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class DefaultCodecProviderFactory
        implements CodecProviderFactory
{
    private static final EmptyCodecProvider EMPTY_CODEC_PROVIDER = new EmptyCodecProvider();

    @Override
    public CodecProvider create(CompressionKind compressionKind, int maxBufferSize, Properties schema)
    {
        requireNonNull(compressionKind, "compressionKind is null");

        switch (compressionKind) {
            case UNCOMPRESSED:
                return EMPTY_CODEC_PROVIDER;
            case ZLIB:
                return new ZlibCodecProvider(maxBufferSize);
            case SNAPPY:
                return new SnappyCodecProvider(maxBufferSize);
            default:
                throw new IllegalStateException(compressionKind + " compression not implemented yet");
        }
    }

    private static class EmptyCodecProvider
            implements CodecProvider
    {
        @Override
        public Codec get(AbstractAggregatedMemoryContext memoryContext)
        {
            return null;
        }

        @Override
        public CompressionKind getCompressionKind()
        {
            return CompressionKind.UNCOMPRESSED;
        }
    }
}
