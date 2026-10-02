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
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.parquet.rule.ParquetDereferencePushDown;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getSynthesizedIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isParquetDereferencePushdownEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IcebergParquetDereferencePushDown
        extends ParquetDereferencePushDown
{
    private final IcebergTransactionManager transactionManager;
    private final IcebergTableProperties tableProperties;

    @Inject
    public IcebergParquetDereferencePushDown(
            IcebergTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            IcebergTableProperties tableProperties)
    {
        super(rowExpressionService);
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
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

        return PARQUET == tableProperties.getFileFormat(session, metadata.getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
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

        // Resolve the subfield against the base column's Presto type. An IcebergColumnHandle only carries a
        // Presto type, so there is no need to go through a Hive type here. Doing so would also be lossy, since
        // Iceberg supports types that Hive cannot represent (for example TIMESTAMP WITH TIME ZONE and UUID) or
        // that do not round-trip (TIME maps onto a Hive bigint).
        Type pushdownColumnType = findSubfieldType(type, subfield);

        return getSynthesizedIcebergColumnHandle(subfieldColumnName, pushdownColumnType, ImmutableList.of(subfield));
    }

    private static Type findSubfieldType(Type baseType, Subfield subfield)
    {
        Type currentType = baseType;
        for (Subfield.PathElement pathElement : subfield.getPath()) {
            checkArgument(pathElement instanceof Subfield.NestedField,
                    "nested column [%s] contains an unsupported path element: %s", subfield, pathElement);
            String fieldName = ((Subfield.NestedField) pathElement).getName();

            if (!(currentType instanceof RowType)) {
                throw new IllegalArgumentException("nested column [" + subfield + "] type is not present in column type " + baseType);
            }

            currentType = ((RowType) currentType).getFields().stream()
                    .filter(field -> field.getName().isPresent() && field.getName().get().equals(fieldName))
                    .map(RowType.Field::getType)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("nested column [" + subfield + "] type is not present in column type " + baseType));
        }
        return currentType;
    }
}
