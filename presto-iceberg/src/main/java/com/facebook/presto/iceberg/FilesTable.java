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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.iceberg.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getTableScan;
import static com.facebook.presto.iceberg.util.PageListBuilder.forTable;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class FilesTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;

    public FilesTable(SchemaTableName tableName, Table icebergTable, Optional<Long> snapshotId, TypeManager typeManager)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("file_path", VARCHAR))
                        .add(new ColumnMetadata("file_format", VARCHAR))
                        .add(new ColumnMetadata("record_count", BIGINT))
                        .add(new ColumnMetadata("file_size_in_bytes", BIGINT))
                        .add(new ColumnMetadata("column_sizes", typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(BIGINT.getTypeSignature())))))
                        .add(new ColumnMetadata("value_counts", typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(BIGINT.getTypeSignature())))))
                        .add(new ColumnMetadata("null_value_counts", typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(BIGINT.getTypeSignature())))))
                        .add(new ColumnMetadata("lower_bounds", typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(VARCHAR.getTypeSignature())))))
                        .add(new ColumnMetadata("upper_bounds", typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(VARCHAR.getTypeSignature())))))
                        .add(new ColumnMetadata("key_metadata", VARBINARY))
                        .add(new ColumnMetadata("split_offsets", new ArrayType(BIGINT)))
                        .build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(tableMetadata, session, icebergTable, snapshotId));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table icebergTable, Optional<Long> snapshotId)
    {
        PageListBuilder pagesBuilder = forTable(tableMetadata);
        TableScan tableScan = getTableScan(TupleDomain.all(), snapshotId, icebergTable).includeColumnStats();
        Map<Integer, Type> idToTypeMap = getIdToTypeMap(icebergTable.schema());

        tableScan.planFiles().forEach(fileScanTask -> {
            DataFile dataFile = fileScanTask.file();
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(dataFile.path().toString());
            pagesBuilder.appendVarchar(dataFile.format().name());
            pagesBuilder.appendBigint(dataFile.recordCount());
            pagesBuilder.appendBigint(dataFile.fileSizeInBytes());
            if (checkNonNull(dataFile.columnSizes(), pagesBuilder)) {
                pagesBuilder.appendIntegerBigintMap(dataFile.columnSizes());
            }
            if (checkNonNull(dataFile.valueCounts(), pagesBuilder)) {
                pagesBuilder.appendIntegerBigintMap(dataFile.valueCounts());
            }
            if (checkNonNull(dataFile.nullValueCounts(), pagesBuilder)) {
                pagesBuilder.appendIntegerBigintMap(dataFile.nullValueCounts());
            }
            if (checkNonNull(dataFile.lowerBounds(), pagesBuilder)) {
                pagesBuilder.appendIntegerVarcharMap(dataFile.lowerBounds().entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry<Integer, ByteBuffer>::getKey,
                                entry -> Transforms.identity(idToTypeMap.get(entry.getKey())).toHumanString(
                                        Conversions.fromByteBuffer(idToTypeMap.get(entry.getKey()), entry.getValue())))));
            }
            if (checkNonNull(dataFile.upperBounds(), pagesBuilder)) {
                pagesBuilder.appendIntegerVarcharMap(dataFile.upperBounds().entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry<Integer, ByteBuffer>::getKey,
                                entry -> Transforms.identity(idToTypeMap.get(entry.getKey())).toHumanString(
                                        Conversions.fromByteBuffer(idToTypeMap.get(entry.getKey()), entry.getValue())))));
            }
            if (checkNonNull(dataFile.keyMetadata(), pagesBuilder)) {
                pagesBuilder.appendVarbinary(Slices.wrappedBuffer(dataFile.keyMetadata()));
            }
            if (checkNonNull(dataFile.splitOffsets(), pagesBuilder)) {
                pagesBuilder.appendBigintArray(dataFile.splitOffsets());
            }
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static Map<Integer, Type> getIdToTypeMap(Schema schema)
    {
        ImmutableMap.Builder<Integer, Type> idToTypeMap = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateIdToTypeMap(field, idToTypeMap);
        }
        return idToTypeMap.build();
    }

    private static void populateIdToTypeMap(Types.NestedField field, ImmutableMap.Builder<Integer, Type> idToTypeMap)
    {
        Type type = field.type();
        idToTypeMap.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateIdToTypeMap(child, idToTypeMap));
        }
    }

    private static boolean checkNonNull(Object object, PageListBuilder pagesBuilder)
    {
        if (object == null) {
            pagesBuilder.appendNull();
            return false;
        }
        return true;
    }
}
