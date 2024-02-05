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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.parquet.rule.ParquetDereferencePushDown;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.iceberg.FileFormat;

import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergColumnHandle.getSynthesizedIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isParquetDereferencePushdownEnabled;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFileFormat;
import static com.facebook.presto.iceberg.TypeConverter.toHiveType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IcebergParquetDereferencePushDown
        extends ParquetDereferencePushDown
{
    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;

    @Inject
    public IcebergParquetDereferencePushDown(
            IcebergTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            TypeManager typeManager)
    {
        super(rowExpressionService);
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected boolean isParquetDereferenceEnabled(ConnectorSession session, TableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorHandle() instanceof IcebergTableHandle,
                "Dereference pushdown into reader is not supported on a non-iceberg TableHandle");

        if (!isParquetDereferencePushdownEnabled(session)) {
            return false;
        }

        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");

        return FileFormat.PARQUET == getFileFormat(metadata.getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
    }

    @Override
    protected String getColumnName(ColumnHandle columnHandle)
    {
        checkArgument(columnHandle instanceof IcebergColumnHandle,
                "Expected Iceberg column handle, instead got: " + columnHandle.getClass());
        return ((IcebergColumnHandle) columnHandle).getName();
    }

    @Override
    protected ColumnHandle createSubfieldColumnHandle(
            ColumnHandle baseColumnHandle,
            Subfield subfield,
            Type subfieldDataType,
            String subfieldColumnName)
    {
        checkArgument(baseColumnHandle instanceof IcebergColumnHandle,
                "Expected Iceberg column handle, instead got: " + baseColumnHandle.getClass());

        IcebergColumnHandle icebergBaseColumnHandle = (IcebergColumnHandle) baseColumnHandle;
        Type type = icebergBaseColumnHandle.getType();
        checkArgument(type instanceof RowType, "%s must be type of RowType", subfield.getRootName());

        Optional<HiveType> nestedColumnHiveType = toHiveType(type)
                .findChildType(subfield.getPath()
                        .stream()
                        .map(p -> ((Subfield.NestedField) p).getName())
                        .collect(Collectors.toList()));

        if (!nestedColumnHiveType.isPresent()) {
            throw new IllegalArgumentException("nested column [" + subfield + "] type is not present in Hive column type");
        }

        Type pushdownColumnType = nestedColumnHiveType.get().getType(typeManager);

        return getSynthesizedIcebergColumnHandle(subfieldColumnName, pushdownColumnType, ImmutableList.of(subfield));
    }
}
