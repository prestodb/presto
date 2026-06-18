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
package com.facebook.presto.orc.metadata;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PostScript
{
    public static final Slice MAGIC = Slices.utf8Slice("ORC");

    public enum HiveWriterVersion
    {
        ORIGINAL(0), ORC_HIVE_8732(1);

        private final int orcWriterVersion;

        HiveWriterVersion(int orcWriterVersion)
        {
            this.orcWriterVersion = orcWriterVersion;
        }

        public int getOrcWriterVersion()
        {
            return orcWriterVersion;
        }
    }

    private final List<Integer> version;
    private final long footerLength;
    private final long metadataLength;
    private final CompressionKind compression;
    private final long compressionBlockSize;
    private final HiveWriterVersion hiveWriterVersion;
    private final OptionalInt dwrfStripeCacheLength;
    private final Optional<DwrfStripeCacheMode> dwrfStripeCacheMode;

    public PostScript(
            List<Integer> version,
            long footerLength,
            long metadataLength,
            CompressionKind compression,
            long compressionBlockSize,
            HiveWriterVersion hiveWriterVersion,
            OptionalInt dwrfStripeCacheLength,
            Optional<DwrfStripeCacheMode> dwrfStripeCacheMode)
    {
        this.version = ImmutableList.copyOf(requireNonNull(version, "version is null"));
        this.footerLength = footerLength;
        this.metadataLength = metadataLength;
        this.compression = requireNonNull(compression, "compressionKind is null");
        this.compressionBlockSize = compressionBlockSize;
        this.hiveWriterVersion = requireNonNull(hiveWriterVersion, "hiveWriterVersion is null");
        this.dwrfStripeCacheLength = requireNonNull(dwrfStripeCacheLength, "dwrfStripeCacheLength is null");
        this.dwrfStripeCacheMode = requireNonNull(dwrfStripeCacheMode, "dwrfStripeCacheMode is null");
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

    public HiveWriterVersion getHiveWriterVersion()
    {
        return hiveWriterVersion;
    }

    public OptionalInt getDwrfStripeCacheLength()
    {
        return dwrfStripeCacheLength;
    }

    public Optional<DwrfStripeCacheMode> getDwrfStripeCacheMode()
    {
        return dwrfStripeCacheMode;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", version)
                .add("footerLength", footerLength)
                .add("metadataLength", metadataLength)
                .add("compressionKind", compression)
                .add("compressionBlockSize", compressionBlockSize)
                .add("hiveWriterVersion", hiveWriterVersion)
                .add("dwrfStripeCacheLength", dwrfStripeCacheLength)
                .add("dwrfStripeCacheMode", dwrfStripeCacheMode)
                .toString();
    }
}
