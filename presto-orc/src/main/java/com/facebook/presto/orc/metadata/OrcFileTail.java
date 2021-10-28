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

import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import io.airlift.slice.Slice;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OrcFileTail
{
    private final HiveWriterVersion hiveWriterVersion;
    private final int bufferSize;
    private final CompressionKind compressionKind;
    private final Slice footerSlice;
    private final int footerSize;
    private final Slice metadataSlice;
    private final int metadataSize;
    private final Optional<DwrfStripeCacheData> dwrfStripeCacheData;

    public OrcFileTail(
            HiveWriterVersion hiveWriterVersion,
            int bufferSize,
            CompressionKind compressionKind,
            Slice footerSlice,
            int footerSize,
            Slice metadataSlice,
            int metadataSize,
            Optional<DwrfStripeCacheData> dwrfStripeCacheData)
    {
        this.hiveWriterVersion = requireNonNull(hiveWriterVersion, "hiveWriterVersion is null");
        this.bufferSize = bufferSize;
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        this.footerSlice = requireNonNull(footerSlice, "footerSlice is null");
        this.footerSize = footerSize;
        this.metadataSlice = requireNonNull(metadataSlice, "metadataSlice is null");
        this.metadataSize = metadataSize;
        this.dwrfStripeCacheData = requireNonNull(dwrfStripeCacheData, "dwrfStripeCacheData is null");
    }

    public HiveWriterVersion getHiveWriterVersion()
    {
        return hiveWriterVersion;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public Slice getFooterSlice()
    {
        return footerSlice;
    }

    public int getFooterSize()
    {
        return footerSize;
    }

    public Slice getMetadataSlice()
    {
        return metadataSlice;
    }

    public int getMetadataSize()
    {
        return metadataSize;
    }

    public Optional<DwrfStripeCacheData> getDwrfStripeCacheData()
    {
        return dwrfStripeCacheData;
    }

    private int getDwrfStripeCacheSize()
    {
        int dwrfStripeCacheSize = 0;
        if (dwrfStripeCacheData.isPresent()) {
            dwrfStripeCacheSize = dwrfStripeCacheData.get().getDwrfStripeCacheSize();
        }
        return dwrfStripeCacheSize;
    }

    public int getTotalSize()
    {
        return getFooterSize() + getMetadataSize() + getDwrfStripeCacheSize();
    }
}
