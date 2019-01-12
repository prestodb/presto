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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.airlift.slice.Slice;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftBigint;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftBigintArray;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftBoolean;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftColumnData;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftDate;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftDouble;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftHyperLogLog;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftInteger;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftJson;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftTimestamp;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftVarchar;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.HYPER_LOG_LOG;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;

@ThriftStruct
public final class PrestoThriftBlock
{
    // number
    private final PrestoThriftInteger integerData;
    private final PrestoThriftBigint bigintData;
    private final PrestoThriftDouble doubleData;

    // variable width
    private final PrestoThriftVarchar varcharData;

    // boolean
    private final PrestoThriftBoolean booleanData;

    // temporal
    private final PrestoThriftDate dateData;
    private final PrestoThriftTimestamp timestampData;

    // special
    private final PrestoThriftJson jsonData;
    private final PrestoThriftHyperLogLog hyperLogLogData;

    // array
    private final PrestoThriftBigintArray bigintArrayData;

    // non-thrift field which points to non-null data item
    private final PrestoThriftColumnData dataReference;

    @ThriftConstructor
    public PrestoThriftBlock(
            @Nullable PrestoThriftInteger integerData,
            @Nullable PrestoThriftBigint bigintData,
            @Nullable PrestoThriftDouble doubleData,
            @Nullable PrestoThriftVarchar varcharData,
            @Nullable PrestoThriftBoolean booleanData,
            @Nullable PrestoThriftDate dateData,
            @Nullable PrestoThriftTimestamp timestampData,
            @Nullable PrestoThriftJson jsonData,
            @Nullable PrestoThriftHyperLogLog hyperLogLogData,
            @Nullable PrestoThriftBigintArray bigintArrayData)
    {
        this.integerData = integerData;
        this.bigintData = bigintData;
        this.doubleData = doubleData;
        this.varcharData = varcharData;
        this.booleanData = booleanData;
        this.dateData = dateData;
        this.timestampData = timestampData;
        this.jsonData = jsonData;
        this.hyperLogLogData = hyperLogLogData;
        this.bigintArrayData = bigintArrayData;
        this.dataReference = theOnlyNonNull(integerData, bigintData, doubleData, varcharData, booleanData, dateData, timestampData, jsonData, hyperLogLogData, bigintArrayData);
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PrestoThriftInteger getIntegerData()
    {
        return integerData;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftBigint getBigintData()
    {
        return bigintData;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public PrestoThriftDouble getDoubleData()
    {
        return doubleData;
    }

    @Nullable
    @ThriftField(value = 4, requiredness = OPTIONAL)
    public PrestoThriftVarchar getVarcharData()
    {
        return varcharData;
    }

    @Nullable
    @ThriftField(value = 5, requiredness = OPTIONAL)
    public PrestoThriftBoolean getBooleanData()
    {
        return booleanData;
    }

    @Nullable
    @ThriftField(value = 6, requiredness = OPTIONAL)
    public PrestoThriftDate getDateData()
    {
        return dateData;
    }

    @Nullable
    @ThriftField(value = 7, requiredness = OPTIONAL)
    public PrestoThriftTimestamp getTimestampData()
    {
        return timestampData;
    }

    @Nullable
    @ThriftField(value = 8, requiredness = OPTIONAL)
    public PrestoThriftJson getJsonData()
    {
        return jsonData;
    }

    @Nullable
    @ThriftField(value = 9, requiredness = OPTIONAL)
    public PrestoThriftHyperLogLog getHyperLogLogData()
    {
        return hyperLogLogData;
    }

    @Nullable
    @ThriftField(value = 10, requiredness = OPTIONAL)
    public PrestoThriftBigintArray getBigintArrayData()
    {
        return bigintArrayData;
    }

    public Block toBlock(Type desiredType)
    {
        return dataReference.toBlock(desiredType);
    }

    public int numberOfRecords()
    {
        return dataReference.numberOfRecords();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftBlock other = (PrestoThriftBlock) obj;
        // remaining fields are guaranteed to be null by the constructor
        return Objects.equals(this.dataReference, other.dataReference);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(integerData, bigintData, doubleData, varcharData, booleanData, dateData, timestampData, jsonData, hyperLogLogData, bigintArrayData);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("data", dataReference)
                .toString();
    }

    public static PrestoThriftBlock integerData(PrestoThriftInteger integerData)
    {
        return new PrestoThriftBlock(integerData, null, null, null, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock bigintData(PrestoThriftBigint bigintData)
    {
        return new PrestoThriftBlock(null, bigintData, null, null, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock doubleData(PrestoThriftDouble doubleData)
    {
        return new PrestoThriftBlock(null, null, doubleData, null, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock varcharData(PrestoThriftVarchar varcharData)
    {
        return new PrestoThriftBlock(null, null, null, varcharData, null, null, null, null, null, null);
    }

    public static PrestoThriftBlock booleanData(PrestoThriftBoolean booleanData)
    {
        return new PrestoThriftBlock(null, null, null, null, booleanData, null, null, null, null, null);
    }

    public static PrestoThriftBlock dateData(PrestoThriftDate dateData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, dateData, null, null, null, null);
    }

    public static PrestoThriftBlock timestampData(PrestoThriftTimestamp timestampData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, timestampData, null, null, null);
    }

    public static PrestoThriftBlock jsonData(PrestoThriftJson jsonData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, null, jsonData, null, null);
    }

    public static PrestoThriftBlock hyperLogLogData(PrestoThriftHyperLogLog hyperLogLogData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, null, null, hyperLogLogData, null);
    }

    public static PrestoThriftBlock bigintArrayData(PrestoThriftBigintArray bigintArrayData)
    {
        return new PrestoThriftBlock(null, null, null, null, null, null, null, null, null, bigintArrayData);
    }

    public static PrestoThriftBlock fromBlock(Block block, Type type)
    {
        switch (type.getTypeSignature().getBase()) {
            case INTEGER:
                return PrestoThriftInteger.fromBlock(block);
            case BIGINT:
                return PrestoThriftBigint.fromBlock(block);
            case DOUBLE:
                return PrestoThriftDouble.fromBlock(block);
            case VARCHAR:
                return PrestoThriftVarchar.fromBlock(block, type);
            case BOOLEAN:
                return PrestoThriftBoolean.fromBlock(block);
            case DATE:
                return PrestoThriftDate.fromBlock(block);
            case TIMESTAMP:
                return PrestoThriftTimestamp.fromBlock(block);
            case JSON:
                return PrestoThriftJson.fromBlock(block, type);
            case HYPER_LOG_LOG:
                return PrestoThriftHyperLogLog.fromBlock(block);
            case ARRAY:
                Type elementType = getOnlyElement(type.getTypeParameters());
                if (BigintType.BIGINT.equals(elementType)) {
                    return PrestoThriftBigintArray.fromBlock(block);
                }
                else {
                    throw new IllegalArgumentException("Unsupported array block type: " + type);
                }
            default:
                throw new IllegalArgumentException("Unsupported block type: " + type);
        }
    }

    public static PrestoThriftBlock fromRecordSetColumn(RecordSet recordSet, int columnIndex, int totalRecords)
    {
        Type type = recordSet.getColumnTypes().get(columnIndex);
        switch (type.getTypeSignature().getBase()) {
            // use more efficient implementations for numeric types which are likely to be used in index join
            case INTEGER:
                return PrestoThriftInteger.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
            case BIGINT:
                return PrestoThriftBigint.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
            case DATE:
                return PrestoThriftDate.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
            case TIMESTAMP:
                return PrestoThriftTimestamp.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
            default:
                // less efficient implementation which converts to a block first
                return fromBlock(convertColumnToBlock(recordSet, columnIndex, totalRecords), type);
        }
    }

    private static Block convertColumnToBlock(RecordSet recordSet, int columnIndex, int positions)
    {
        Type type = recordSet.getColumnTypes().get(columnIndex);
        BlockBuilder output = type.createBlockBuilder(null, positions);
        Class<?> javaType = type.getJavaType();
        RecordCursor cursor = recordSet.cursor();
        for (int position = 0; position < positions; position++) {
            checkState(cursor.advanceNextPosition(), "cursor has less values than expected");
            if (cursor.isNull(columnIndex)) {
                output.appendNull();
            }
            else {
                if (javaType == boolean.class) {
                    type.writeBoolean(output, cursor.getBoolean(columnIndex));
                }
                else if (javaType == long.class) {
                    type.writeLong(output, cursor.getLong(columnIndex));
                }
                else if (javaType == double.class) {
                    type.writeDouble(output, cursor.getDouble(columnIndex));
                }
                else if (javaType == Slice.class) {
                    Slice slice = cursor.getSlice(columnIndex);
                    type.writeSlice(output, slice, 0, slice.length());
                }
                else {
                    type.writeObject(output, cursor.getObject(columnIndex));
                }
            }
        }
        checkState(!cursor.advanceNextPosition(), "cursor has more values than expected");
        return output.build();
    }

    private static PrestoThriftColumnData theOnlyNonNull(PrestoThriftColumnData... columnsData)
    {
        PrestoThriftColumnData result = null;
        for (PrestoThriftColumnData data : columnsData) {
            if (data != null) {
                checkArgument(result == null, "more than one type is present");
                result = data;
            }
        }
        checkArgument(result != null, "no types are present");
        return result;
    }
}
