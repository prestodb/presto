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

package io.prestosql.connector.system;

import com.google.common.collect.ImmutableSet;
import io.prestosql.FullConnectorSession;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class MetadataBasedSystemTablesProvider
        implements SystemTablesProvider
{
    private final Metadata metadata;
    private final String catalogName;

    public MetadataBasedSystemTablesProvider(Metadata metadata, String catalogName)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public Set<SystemTable> listSystemTables(ConnectorSession session)
    {
        return ImmutableSet.of();
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<SystemTable> systemTable = metadata.getSystemTable(
                ((FullConnectorSession) session).getSession(),
                new QualifiedObjectName(catalogName, tableName.getSchemaName(), tableName.getTableName()));

        // dynamic system tables require access to the transaction and thus can only run on the current coordinator
        if (systemTable.isPresent() && systemTable.get().getDistribution() != SINGLE_COORDINATOR) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Distribution for dynamic system table must be " + SINGLE_COORDINATOR);
        }

        return systemTable;
    }
}
