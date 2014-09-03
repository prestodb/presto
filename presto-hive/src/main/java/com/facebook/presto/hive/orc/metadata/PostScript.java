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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PostScript
{
    private final List<Integer> version;
    private final long footerLength;
    private final long metadataLength;
    private final CompressionKind compression;
    private final long compressionBlockSize;

    public PostScript(List<Integer> version, long footerLength, long metadataLength, CompressionKind compression, long compressionBlockSize)
    {
        this.version = ImmutableList.copyOf(checkNotNull(version, "version is null"));
        this.footerLength = footerLength;
        this.metadataLength = metadataLength;
        this.compression = checkNotNull(compression, "compressionKind is null");
        this.compressionBlockSize = compressionBlockSize;
    }

    public List<Integer> getVersion()
    {
        return version;
    }

    public long getFooterLength()
    {
        return footerLength;
    }

    public long getMetadataLength()
    {
        return metadataLength;
    }

    public CompressionKind getCompression()
    {
        return compression;
    }

    public long getCompressionBlockSize()
    {
        return compressionBlockSize;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("version", version)
                .add("footerLength", footerLength)
                .add("metadataLength", metadataLength)
                .add("compressionKind", compression)
                .add("compressionBlockSize", compressionBlockSize)
                .toString();
    }
}
