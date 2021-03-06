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
package com.facebook.presto.raptor.storage;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.WriterStats;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.HASHED;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_WRITER_DATA_ERROR;
import static com.facebook.presto.raptor.storage.OrcStorageManager.DEFAULT_STORAGE_TIMEZONE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OrcFileWriter
        implements FileWriter
{
    public static final OrcWriterOptions DEFAULT_OPTION = new OrcWriterOptions();
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    private final OrcWriter orcWriter;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;

    public OrcFileWriter(List<Long> columnIds, List<Type> columnTypes, DataSink target, boolean validate, WriterStats stats, TypeManager typeManager, CompressionKind compression)
    {
        this(columnIds, columnTypes, target, true, validate, stats, typeManager, compression);
    }

    @VisibleForTesting
    OrcFileWriter(
            List<Long> columnIds,
            List<Type> columnTypes,
            DataSink target,
            boolean writeMetadata,
            boolean validate,
            WriterStats stats,
            TypeManager typeManager,
            CompressionKind compression)
    {
        checkArgument(requireNonNull(columnIds, "columnIds is null").size() == requireNonNull(columnTypes, "columnTypes is null").size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        StorageTypeConverter converter = new StorageTypeConverter(typeManager);
        List<Type> storageTypes = columnTypes.stream()
                .map(converter::toStorageType)
                .collect(toImmutableList());
        List<String> columnNames = columnIds.stream().map(Object::toString).collect(toImmutableList());

        Map<String, String> userMetadata = ImmutableMap.of();
        if (writeMetadata) {
            ImmutableMap.Builder<Long, TypeSignature> columnTypesMap = ImmutableMap.builder();
            for (int i = 0; i < columnIds.size(); i++) {
                columnTypesMap.put(columnIds.get(i), columnTypes.get(i).getTypeSignature());
            }
            userMetadata = ImmutableMap.of(OrcFileMetadata.KEY, METADATA_CODEC.toJson(new OrcFileMetadata(columnTypesMap.build())));
        }

        try {
            orcWriter = new OrcWriter(
                    target,
                    columnNames,
                    storageTypes,
                    ORC,
                    requireNonNull(compression, "compression is null"),
                    Optional.empty(),
                    NO_ENCRYPTION,
                    DEFAULT_OPTION,
                    userMetadata,
                    DEFAULT_STORAGE_TIMEZONE,
                    validate,
                    HASHED,
                    stats);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
    }

    @Override
    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            try {
                orcWriter.write(page);
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(RAPTOR_WRITER_DATA_ERROR, e);
            }
            uncompressedSize += page.getLogicalSizeInBytes();
            rowCount += page.getPositionCount();
        }
    }

    @Override
    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = inputPages.get(pageIndexes[i]);
            // This will do data copy; be aware
            Page singleValuePage = page.getSingleValuePage(positionIndexes[i]);
            try {
                orcWriter.write(singleValuePage);
                uncompressedSize += singleValuePage.getLogicalSizeInBytes();
                rowCount++;
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(RAPTOR_WRITER_DATA_ERROR, e);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        orcWriter.close();
    }

    @Override
    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }
}
