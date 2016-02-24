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
package com.facebook.presto.raptor.systemtables;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import static com.facebook.presto.raptor.systemtables.TableMetadataPageSource.SCHEMA_NAME;
import static com.facebook.presto.raptor.systemtables.TableMetadataPageSource.TABLE_NAME;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableMetadataSystemTable
        implements SystemTable
{
    private final IDBI dbi;
    private final ConnectorTableMetadata tableMetadata;

    @Inject
    public TableMetadataSystemTable(@ForMetadata IDBI dbi, TypeManager typeManager)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        requireNonNull(typeManager, "typeManager is null");

        Type arrayOfVarchar = typeManager.getType(parseTypeSignature("array<varchar>"));
        this.tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "tables"),
                ImmutableList.of(
                        new ColumnMetadata(SCHEMA_NAME, VARCHAR),
                        new ColumnMetadata(TABLE_NAME, VARCHAR),
                        new ColumnMetadata("temporal_column", VARCHAR),
                        new ColumnMetadata("ordering_columns", arrayOfVarchar),
                        new ColumnMetadata("distribution_name", VARCHAR),
                        new ColumnMetadata("bucket_count", BIGINT),
                        new ColumnMetadata("bucketing_columns", arrayOfVarchar)));
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new TableMetadataPageSource(dbi, constraint, tableMetadata);
    }
}
