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

import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcOutputBuffer;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CompressedMetadataWriter
{
    private final MetadataWriter metadataWriter;
    private final OrcOutputBuffer buffer;

    public CompressedMetadataWriter(MetadataWriter metadataWriter, ColumnWriterOptions columnWriterOptions, Optional<DwrfDataEncryptor> dwrfEncryptor)
    {
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.buffer = new OrcOutputBuffer(columnWriterOptions, dwrfEncryptor);
    }

    public List<Integer> getOrcMetadataVersion()
    {
        return metadataWriter.getOrcMetadataVersion();
    }

    public Slice writePostscript(int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize, Optional<DwrfStripeCacheData> dwrfStripeCacheData)
            throws IOException
    {
        // postscript is not compressed
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        metadataWriter.writePostscript(output, footerLength, metadataLength, compression, compressionBlockSize, dwrfStripeCacheData);
        return output.slice();
    }

    public Slice writeDwrfStripeCache(Optional<DwrfStripeCacheData> dwrfStripeCacheData)
            throws IOException
    {
        // DWRF stripe cache is already compressed
        int size = dwrfStripeCacheData.map(DwrfStripeCacheData::getDwrfStripeCacheSize).orElse(0);
        DynamicSliceOutput output = new DynamicSliceOutput(size);
        metadataWriter.writeDwrfStripeCache(output, dwrfStripeCacheData);
        return output.slice();
    }

    public Slice writeMetadata(Metadata metadata)
            throws IOException
    {
        metadataWriter.writeMetadata(buffer, metadata);
        return getSliceOutput();
    }

    public Slice writeFooter(Footer footer)
            throws IOException
    {
        metadataWriter.writeFooter(buffer, footer);
        return getSliceOutput();
    }

    public Slice writeStripeFooter(StripeFooter footer)
            throws IOException
    {
        metadataWriter.writeStripeFooter(buffer, footer);
        return getSliceOutput();
    }

    public Slice writeRowIndexes(List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        metadataWriter.writeRowIndexes(buffer, rowGroupIndexes);
        return getSliceOutput();
    }

    private Slice getSliceOutput()
    {
        buffer.close();
        DynamicSliceOutput output = new DynamicSliceOutput(toIntExact(buffer.getOutputDataSize()));
        buffer.writeDataTo(output);
        Slice slice = output.slice();
        buffer.reset();
        return slice;
    }
}
