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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.ColumnFileHandle;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RaptorRecordSink
        implements RecordSink
{
    private final String nodeId;
    private final ColumnFileHandle fileHandle;
    private final StorageManager storageManager;
    private final List<Type> columnTypes;
    private final PageBuilder pageBuilder;
    private final int sampleWeightField;

    private int field = -1;

    public RaptorRecordSink(String nodeId, ColumnFileHandle fileHandle, StorageManager storageManager, List<Type> columnTypes, RaptorColumnHandle sampleWeightColumnHandle)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.fileHandle = checkNotNull(fileHandle, "fileHandle is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));

        if (sampleWeightColumnHandle != null) {
            checkArgument(sampleWeightColumnHandle.getColumnName().equals(RaptorColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME), "sample weight column handle has wrong name");
            // sample weight is always stored last in the table
            sampleWeightField = columnTypes.size() - 1;
        }
        else {
            sampleWeightField = -1;
        }
        pageBuilder = new PageBuilder(toTypes(columnTypes));
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        checkState(field == -1, "already in record");
        field = 0;
        if (sampleWeightField >= 0) {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(sampleWeightField), sampleWeight);
        }
    }

    private int lastField()
    {
        return fileHandle.getFieldCount() - (sampleWeightField != -1 ? 1 : 0);
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == lastField(), "not all fields set");
        field = -1;

        if (pageBuilder.isFull()) {
            fileHandle.append(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    @Override
    public void appendNull()
    {
        nextBlockBuilder().appendNull();
    }

    @Override
    public void appendBoolean(boolean value)
    {
        columnTypes.get(field).writeBoolean(nextBlockBuilder(), value);
    }

    @Override
    public void appendLong(long value)
    {
        columnTypes.get(field).writeLong(nextBlockBuilder(), value);
    }

    @Override
    public void appendDouble(double value)
    {
        columnTypes.get(field).writeDouble(nextBlockBuilder(), value);
    }

    @Override
    public void appendString(byte[] value)
    {
        Slice slice = Slices.wrappedBuffer(value);
        columnTypes.get(field).writeSlice(nextBlockBuilder(), slice, 0, slice.length());
    }

    @Override
    public String commit()
    {
        checkState(field == -1, "record not finished");

        if (!pageBuilder.isEmpty()) {
            fileHandle.append(pageBuilder.build());
        }

        try {
            storageManager.commit(fileHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return Joiner.on(':').join(nodeId, fileHandle.getShardUuid());
    }

    private BlockBuilder nextBlockBuilder()
    {
        checkState(field != -1, "not in record");
        checkState(field < lastField(), "all fields already set");
        BlockBuilder builder = pageBuilder.getBlockBuilder(field);
        field++;
        return builder;
    }

    private static List<Type> toTypes(List<Type> columnTypes)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (Type columnType : columnTypes) {
            types.add(columnType);
        }
        return types.build();
    }
}
