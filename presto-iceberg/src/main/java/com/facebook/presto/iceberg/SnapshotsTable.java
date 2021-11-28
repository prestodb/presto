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
import org.apache.iceberg.Table;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SnapshotsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        requireNonNull(typeManager, "typeManager is null");

        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("committed_at", TIMESTAMP_WITH_TIME_ZONE))
                        .add(new ColumnMetadata("snapshot_id", BIGINT))
                        .add(new ColumnMetadata("parent_id", BIGINT))
                        .add(new ColumnMetadata("operation", VARCHAR))
                        .add(new ColumnMetadata("manifest_list", VARCHAR))
                        .add(new ColumnMetadata("summary", typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(VARCHAR.getTypeSignature()),
                                TypeSignatureParameter.of(VARCHAR.getTypeSignature())))))
                        .build());
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
        return new FixedPageSource(buildPages(tableMetadata, session, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        icebergTable.snapshots().forEach(snapshot -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendTimestampTzMillis(snapshot.timestampMillis(), session.getSqlFunctionProperties().getTimeZoneKey());
            pagesBuilder.appendBigint(snapshot.snapshotId());
            if (checkNonNull(snapshot.parentId(), pagesBuilder)) {
                pagesBuilder.appendBigint(snapshot.parentId());
            }
            if (checkNonNull(snapshot.operation(), pagesBuilder)) {
                pagesBuilder.appendVarchar(snapshot.operation());
            }
            if (checkNonNull(snapshot.manifestListLocation(), pagesBuilder)) {
                pagesBuilder.appendVarchar(snapshot.manifestListLocation());
            }
            if (checkNonNull(snapshot.summary(), pagesBuilder)) {
                pagesBuilder.appendVarcharVarcharMap(snapshot.summary());
            }
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
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
