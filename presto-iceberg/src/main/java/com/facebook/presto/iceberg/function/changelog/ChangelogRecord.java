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

package com.facebook.presto.iceberg.function.changelog;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.SingleRowBlock;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.changelog.ChangelogOperation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.changelog.ChangelogOperation.INSERT;
import static java.util.Objects.requireNonNull;

public class ChangelogRecord
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ChangelogRecord.class).instanceSize();
    private Block lastRow;
    private Slice lastOperation;
    private int lastOrdinal;

    private final Type serializedType;
    private final Type stateType;

    private static Type getSerializedRowType(Type inner)
    {
        return RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR, inner));
    }

    public ChangelogRecord(Type stateType)
    {
        this.stateType = requireNonNull(stateType, "type is null");
        this.serializedType = getSerializedRowType(stateType);
        this.lastOperation = Slices.EMPTY_SLICE;
        this.lastOrdinal = -1; // ensures this assumes the values from the first merge.
    }

    public ChangelogRecord(Type stateType, int lastOrdinal, Slice lastOperation, Block lastRow)
    {
        this(stateType);
        this.lastOrdinal = lastOrdinal;
        this.lastOperation = lastOperation;
        this.lastRow = lastRow;
    }

    public void add(Integer ordinal, Slice operation, Block row)
    {
        merge(new ChangelogRecord(stateType, ordinal, operation, row));
    }

    public Block getRow()
    {
        return lastRow;
    }

    public Slice getLastOperation()
    {
        return lastOperation;
    }

    public ChangelogRecord merge(ChangelogRecord other)
    {
        if (other.lastOrdinal > lastOrdinal) {
            this.lastOperation = other.lastOperation;
            this.lastRow = other.lastRow;
            this.lastOrdinal = other.lastOrdinal;
        }
        else if (other.lastOrdinal == lastOrdinal) {
            // ordinals are equal. In the case both operations are inserts, we
            // don't have a way to break ties. Likely an error, throw exception
            ChangelogOperation operation = ChangelogOperation.valueOf(other.lastOperation.toStringUtf8().toUpperCase());
            switch (operation) {
                case UPDATE_AFTER:
                case INSERT:
                    if (ChangelogOperation.valueOf(lastOperation.toStringUtf8().toUpperCase()).equals(INSERT)) {
                        throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "unresolvable order for two inserts");
                    }
                    lastOperation = other.lastOperation;
                    lastRow = other.lastRow;
                    lastOrdinal = other.lastOrdinal;
                    break;
                case UPDATE_BEFORE:
                case DELETE:
                    // we don't need to record data before a record update/delete
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "unsupported operation type " + operation);
            }
        }
        return this;
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder entry = out.beginBlockEntry();
        BIGINT.writeLong(entry, lastOrdinal);
        VARCHAR.writeSlice(entry, lastOperation);
        stateType.appendTo(lastRow, 0, entry);
        out.closeEntry();
    }

    /**
     * Sets the inner values of this object to the serialized state contained
     * within the {@link Block} parameter.
     *
     * @param block block containing the serialized state
     * @param index index in the block containing the state
     */
    public void deserialize(Block block, int index)
    {
        Block serializedStateBlock = block.getSingleValueBlock(index);
        if (!(serializedStateBlock instanceof RowBlock)) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "changelog deserialization must only be row block type");
        }
        SingleRowBlock row = (SingleRowBlock) serializedType.getObject(serializedStateBlock, 0);
        lastOrdinal = (int) row.getLong(0);
        lastOperation = row.getSlice(1, 0, row.getSliceLength(1));
        lastRow = row.getSingleValueBlock(2);
    }

    public long getEstimatedSize()
    {
        long size = INSTANCE_SIZE + IntegerType.INTEGER.getFixedSize();
        if (lastOperation != null) {
            size += lastOperation.getRetainedSize();
        }
        if (lastRow != null) {
            size += lastRow.getSizeInBytes();
        }
        return size;
    }
}
