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
package io.prestosql.plugin.raptor.legacy.systemtables;

import io.prestosql.plugin.raptor.legacy.metadata.ForMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import static io.prestosql.plugin.raptor.legacy.systemtables.ShardMetadataRecordCursor.SHARD_METADATA;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class ShardMetadataSystemTable
        implements SystemTable
{
    private final IDBI dbi;

    @Inject
    public ShardMetadataSystemTable(@ForMetadata IDBI dbi)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return SHARD_METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new ShardMetadataRecordCursor(dbi, constraint);
    }
}
