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

import com.facebook.presto.orc.OrcOutputBuffer;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.util.List;

public class CompressedMetadataWriter
        implements MetadataWriter
{
    private final MetadataWriter metadataWriter;
    private final OrcOutputBuffer buffer;

    public CompressedMetadataWriter(MetadataWriter metadataWriter, CompressionKind compression, int bufferSize)
    {
        this.metadataWriter = metadataWriter;
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    @Override
    public List<Integer> getOrcMetadataVersion()
    {
        return metadataWriter.getOrcMetadataVersion();
    }

    @Override
    public int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException
    {
        // postscript is not compressed
        return metadataWriter.writePostscript(output, footerLength, metadataLength, compression, compressionBlockSize);
    }

    @Override
    public int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeMetadata(buffer, metadata);
        return buffer.writeDataTo(output);
    }

    @Override
    public int writeFooter(SliceOutput output, Footer footer)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeFooter(buffer, footer);
        return buffer.writeDataTo(output);
    }

    @Override
    public int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeStripeFooter(buffer, footer);
        return buffer.writeDataTo(output);
    }

    @Override
    public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeRowIndexes(buffer, rowGroupIndexes);
        return buffer.writeDataTo(output);
    }
}
