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
package io.prestosql.orc.metadata;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.prestosql.orc.OrcOutputBuffer;

import java.io.IOException;
import java.util.List;

import static java.lang.Math.toIntExact;

public class CompressedMetadataWriter
{
    private final MetadataWriter metadataWriter;
    private final OrcOutputBuffer buffer;

    public CompressedMetadataWriter(MetadataWriter metadataWriter, CompressionKind compression, int bufferSize)
    {
        this.metadataWriter = metadataWriter;
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    public List<Integer> getOrcMetadataVersion()
    {
        return metadataWriter.getOrcMetadataVersion();
    }

    public Slice writePostscript(int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException
    {
        // postscript is not compressed
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        metadataWriter.writePostscript(output, footerLength, metadataLength, compression, compressionBlockSize);
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
