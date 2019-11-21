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

import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.RowFieldName;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.raptor.RaptorColumnHandle.isBucketNumberColumn;
import static com.facebook.presto.raptor.RaptorColumnHandle.isShardRowIdColumn;
import static com.facebook.presto.raptor.RaptorColumnHandle.isShardUuidColumn;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.OrcPageSource.BUCKET_NUMBER_COLUMN;
import static com.facebook.presto.raptor.storage.OrcPageSource.ROWID_COLUMN;
import static com.facebook.presto.raptor.storage.OrcPageSource.SHARD_UUID_COLUMN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static org.joda.time.DateTimeZone.UTC;

public class StorageManagerUtils
{
    private StorageManagerUtils() {}

    // Raptor does not store time-related data types as they are in ORC.
    // They will be casted to BIGINT or INTEGER to avoid timezone conversion.
    // This is due to the historical reason of using the legacy ORC read/writer that does not support timestamp types.
    // In order to be consistent, we still enforce the conversion.
    // The following DEFAULT_STORAGE_TIMEZONE is not used by the optimized ORC read/writer given we never read/write timestamp types.
    public static final DateTimeZone DEFAULT_STORAGE_TIMEZONE = UTC;
    // TODO: do not limit the max size of blocks to read for now; enable the limit when the Hive connector is ready
    public static final DataSize HUGE_MAX_READ_BLOCK_SIZE = new DataSize(1, PETABYTE);

    public static int toSpecialIndex(long columnId)
    {
        if (isShardRowIdColumn(columnId)) {
            return ROWID_COLUMN;
        }
        if (isShardUuidColumn(columnId)) {
            return SHARD_UUID_COLUMN;
        }
        if (isBucketNumberColumn(columnId)) {
            return BUCKET_NUMBER_COLUMN;
        }
        throw new PrestoException(RAPTOR_ERROR, "Invalid column ID: " + columnId);
    }

    public static Type getType(TypeManager typeManager, List<OrcType> types, int index)
    {
        OrcType type = types.get(index);
        switch (type.getOrcTypeKind()) {
            case BOOLEAN:
                return BOOLEAN;
            case LONG:
                return BIGINT;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return createUnboundedVarcharType();
            case VARCHAR:
                return createVarcharType(type.getLength().get());
            case CHAR:
                return createCharType(type.getLength().get());
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                return DecimalType.createDecimalType(type.getPrecision().get(), type.getScale().get());
            case LIST:
                TypeSignature elementType = getType(typeManager, types, type.getFieldTypeIndex(0)).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType)));
            case MAP:
                TypeSignature keyType = getType(typeManager, types, type.getFieldTypeIndex(0)).getTypeSignature();
                TypeSignature valueType = getType(typeManager, types, type.getFieldTypeIndex(1)).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
            case STRUCT:
                List<String> fieldNames = type.getFieldNames();
                ImmutableList.Builder<TypeSignatureParameter> fieldTypes = ImmutableList.builder();
                for (int i = 0; i < type.getFieldCount(); i++) {
                    fieldTypes.add(TypeSignatureParameter.of(new NamedTypeSignature(
                            Optional.of(new RowFieldName(fieldNames.get(i), false)),
                            getType(typeManager, types, type.getFieldTypeIndex(i)).getTypeSignature())));
                }
                return typeManager.getParameterizedType(StandardTypes.ROW, fieldTypes.build());
        }
        throw new PrestoException(RAPTOR_ERROR, "Unhandled ORC type: " + type);
    }

    public static Type toOrcFileType(Type raptorType, TypeManager typeManager)
    {
        // TIMESTAMPS are stored as BIGINT to void the poor encoding in ORC
        if (raptorType == TimestampType.TIMESTAMP) {
            return BIGINT;
        }
        if (raptorType instanceof ArrayType) {
            Type elementType = toOrcFileType(((ArrayType) raptorType).getElementType(), typeManager);
            return new ArrayType(elementType);
        }
        if (raptorType instanceof MapType) {
            TypeSignature keyType = toOrcFileType(((MapType) raptorType).getKeyType(), typeManager).getTypeSignature();
            TypeSignature valueType = toOrcFileType(((MapType) raptorType).getValueType(), typeManager).getTypeSignature();
            return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
        }
        if (raptorType instanceof RowType) {
            List<RowType.Field> fields = ((RowType) raptorType).getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), toOrcFileType(field.getType(), typeManager)))
                    .collect(toImmutableList());
            return RowType.from(fields);
        }
        return raptorType;
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, Integer> indexMap)
    {
        ImmutableList.Builder<ColumnReference<RaptorColumnHandle>> columns = ImmutableList.builder();
        for (RaptorColumnHandle column : effectivePredicate.getDomains().get().keySet()) {
            Integer index = indexMap.get(column.getColumnId());
            if (index != null) {
                columns.add(new ColumnReference<>(column, index, column.getColumnType()));
            }
        }
        return new TupleDomainOrcPredicate<>(effectivePredicate, columns.build(), false, Optional.empty());
    }

    private static Map<Long, Integer> columnIdIndex(List<String> columnNames)
    {
        ImmutableMap.Builder<Long, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(Long.valueOf(columnNames.get(i)), i);
        }
        return map.build();
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException ignored) {
        }
    }
}
