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

import com.facebook.presto.orc.OrcDataSink;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.raptor.storage.organization.PageIndexInfo;
import com.facebook.presto.raptor.storage.organization.SortedRowSource;
import com.facebook.presto.raptor.util.SyncingFileOutputStream;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class OrcFileWriter
        implements Closeable
{
    private final List<Type> columnTypes;
    private final PageBuilder pageBuilder;
    private final OrcWriter orcWriter;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;


    public OrcFileWriter(List<Long> columnIds, List<Type> columnTypes, File target, TypeManager typeManager)
    {
        this(columnIds, columnTypes, target, typeManager, CompressionType.SNAPPY);
    }

    @VisibleForTesting
    OrcFileWriter(List<Long> columnIds, List<Type> columnTypes, File target, TypeManager typeManager, CompressionType compressionType)
    {
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        StorageTypeConverter converter = new StorageTypeConverter(typeManager);
        List<Type> storageTypes = columnTypes.stream()
                .map(converter::toStorageType)
                .collect(toImmutableList());
        this.pageBuilder = new PageBuilder(storageTypes);

        Map<String, String> metadata = OrcFileMetadata.from(columnIds, columnTypes).toMap();
        this.orcWriter = createOrcFileWriter(target, columnIds, storageTypes, compressionType, metadata, new OrcWriterStats());

    }
    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            appendPage(page);
        }
    }

    public void appendPages(List<Page> pages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");

        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = pages.get(pageIndexes[i]);
            int position = positionIndexes[i];
            appendPositionTo(page, position, pageBuilder);

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    public void appendPageIndexInfos(List<PageIndexInfo> pageIndexInfo)
    {
        for (int i = 0; i < pageIndexInfo.size(); i++) {
            Page page = pageIndexInfo.get(i).getCurrentPage();
            int position = pageIndexInfo.get(i).getCurrentPosition();
            appendPositionTo(page, position, pageBuilder);

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    public void appendRow(Row row)
    {
        throw new PrestoException(RAPTOR_ERROR, "appendRow not supported in OrcFileWriter");
//        List<Object> columns = row.getColumns();
//        checkArgument(columns.size() == columnTypes.size());
//        for (int channel = 0; channel < columns.size(); channel++) {
//            tableInspector.setStructFieldData(orcRow, structFields.get(channel), columns.get(channel));
//        }
//        try {
//            recordWriter.write(serializer.serialize(orcRow, tableInspector));
//        }
//        catch (IOException e) {
//            throw new PrestoException(RAPTOR_ERROR, "Failed to write record", e);
//        }
//        rowCount++;
//        uncompressedSize += row.getSizeInBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            orcWriter.close();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private void appendPage(Page page)
    {
        rowCount += page.getPositionCount();
        uncompressedSize += page.getSizeInBytes();

        try {
            orcWriter.write(page);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to write data", e);
        }
    }

    private static void appendPositionTo(Page page, int position, PageBuilder pageBuilder)
    {
        pageBuilder.declarePosition();
        for (int i = 0; i < page.getChannelCount(); i++) {
            Type type = pageBuilder.getType(i);
            Block block = page.getBlock(i);
            BlockBuilder output = pageBuilder.getBlockBuilder(i);
            type.appendTo(block, position, output);
        }
    }

    public static OrcDataSink createOrcDataSink(File target)
    {
        try {
            return new OutputStreamOrcDataSink(new SyncingFileOutputStream(target));
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open output file: " + target, e);
        }
    }

    public static OrcWriter createOrcFileWriter(
            File target,
            List<Long> columnIds,
            List<Type> storageTypes,
            CompressionType compressionType,
            Map<String, String> metadata,
            OrcWriterStats stats)
    {
        checkArgument(columnIds.size() == storageTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        List<String> columnNames = columnIds.stream()
                .map(Object::toString)
                .collect(toImmutableList());

        return new OrcWriter(
                createOrcDataSink(target),
                columnNames,
                storageTypes,
                ORC,
                compressionType.compressionKind(),
                new com.facebook.presto.orc.OrcWriterOptions(),
                metadata,
                UTC,
                true,
                BOTH,
                stats);
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }
}
