
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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfEncryptionInfo;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.util.List;

import static com.facebook.presto.orc.writer.ColumnWriters.createColumnWriter;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FlatMapValueWriterFactory
{
    private final int columnIndex;
    private final List<OrcType> orcTypes;
    private final Type type;
    private final ColumnWriterOptions columnWriterOptions;
    private final OrcEncoding orcEncoding;
    private final DateTimeZone hiveStorageTimeZone;
    private final DwrfEncryptionInfo dwrfEncryptors;
    private final MetadataWriter metadataWriter;

    public FlatMapValueWriterFactory(
            final int columnIndex,
            final List<OrcType> orcTypes,
            final Type type,
            final ColumnWriterOptions columnWriterOptions,
            final OrcEncoding orcEncoding,
            final DateTimeZone hiveStorageTimeZone,
            final DwrfEncryptionInfo dwrfEncryptors,
            final MetadataWriter metadataWriter)
    {
        checkState(columnIndex >= 0, "column index is negative");
        requireNonNull(orcTypes, "orcTypes is null");
        requireNonNull(type, "type is null");
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        requireNonNull(metadataWriter, "metadataWriter is null");

        this.columnIndex = columnIndex;
        this.orcTypes = orcTypes;
        this.type = type;
        // disable dictionary encoding
        this.columnWriterOptions =
                new ColumnWriterOptions(columnWriterOptions.getCompressionKind(),
                        columnWriterOptions.getCompressionLevel(),
                        new DataSize(columnWriterOptions.getCompressionMaxBufferSize(), DataSize.Unit.BYTE),
                        columnWriterOptions.getStringStatisticsLimit(),
                        false);
        this.orcEncoding = orcEncoding;
        this.hiveStorageTimeZone = hiveStorageTimeZone;
        this.dwrfEncryptors = dwrfEncryptors;
        this.metadataWriter = metadataWriter;
    }

    ColumnWriter getColumnWriter(int dwrfSequence)
    {
        checkState(dwrfSequence >= 1, "sequence should be positive");
        ColumnWriter columnWriter = createColumnWriter(
                columnIndex,
                dwrfSequence,
                orcTypes,
                type,
                columnWriterOptions,
                orcEncoding,
                hiveStorageTimeZone,
                dwrfEncryptors,
                metadataWriter);
        return columnWriter;
    }

    public FlatMapValueColumnWriter getFlatMapValueColumnWriter(int dwrfSequence)
    {
        FlatMapValueColumnWriter flatMapValueColumnWriter = new FlatMapValueColumnWriter(
                columnIndex,
                dwrfSequence,
                columnWriterOptions,
                dwrfEncryptors.getEncryptorByNodeId(columnIndex),
                metadataWriter,
                getColumnWriter(dwrfSequence));
        return flatMapValueColumnWriter;
    }
}
