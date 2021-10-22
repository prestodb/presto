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
package com.facebook.presto.hive.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.parquet.rule.ParquetDereferencePushDown;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetDereferencePushdownEnabled;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Extending Parquet dereference pushdown rule to work with Hive connector.
 */
public class HiveParquetDereferencePushDown
        extends ParquetDereferencePushDown
{
    private final HiveTransactionManager transactionManager;

    public HiveParquetDereferencePushDown(
            HiveTransactionManager transactionManager,
            RowExpressionService rowExpressionService)
    {
        super(rowExpressionService);
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    protected boolean isParquetDereferenceEnabled(ConnectorSession session, TableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorHandle() instanceof HiveTableHandle,
                "Dereference pushdown into reader is not supported on a non-hive TableHandle");

        if (!isParquetDereferencePushdownEnabled(session)) {
            return false;
        }

        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof HiveMetadata, "metadata must be HiveMetadata");

        return PARQUET == getHiveStorageFormat(metadata
                .getTableMetadata(session, tableHandle.getConnectorHandle())
                .getProperties());
    }

    @Override
    protected String getColumnName(ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getName();
    }

    @Override
    protected ColumnHandle createSubfieldColumnHandle(
            ColumnHandle baseColumnHandle,
            Subfield subfield,
            Type subfieldDataType,
            String subfieldColumnName)
    {
        if (baseColumnHandle == null) {
            throw new IllegalArgumentException("nested column [" + subfield + "]'s base column " +
                    subfield.getRootName() + " is not present in table scan output");
        }
        HiveColumnHandle hiveBaseColumnHandle = (HiveColumnHandle) baseColumnHandle;

        Optional<HiveType> nestedColumnHiveType = hiveBaseColumnHandle
                .getHiveType()
                .findChildType(
                        subfield.getPath().stream()
                                .map(p -> ((Subfield.NestedField) p).getName())
                                .collect(Collectors.toList()));

        if (!nestedColumnHiveType.isPresent()) {
            throw new IllegalArgumentException(
                    "nested column [" + subfield + "] type is not present in Hive column type");
        }

        // Create column handle for subfield column
        return new HiveColumnHandle(
                subfieldColumnName,
                nestedColumnHiveType.get(),
                subfieldDataType.getTypeSignature(),
                -1,
                SYNTHESIZED,
                Optional.of("nested column pushdown"),
                ImmutableList.of(subfield),
                Optional.empty());
    }
}
