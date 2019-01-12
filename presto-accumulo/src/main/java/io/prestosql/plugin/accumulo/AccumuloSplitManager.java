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
package io.prestosql.plugin.accumulo;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.accumulo.model.AccumuloColumnConstraint;
import io.prestosql.plugin.accumulo.model.AccumuloColumnHandle;
import io.prestosql.plugin.accumulo.model.AccumuloSplit;
import io.prestosql.plugin.accumulo.model.AccumuloTableHandle;
import io.prestosql.plugin.accumulo.model.AccumuloTableLayoutHandle;
import io.prestosql.plugin.accumulo.model.TabletSplitMetadata;
import io.prestosql.plugin.accumulo.model.WrappedRange;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.TupleDomain.ColumnDomain;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AccumuloSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final AccumuloClient client;

    @Inject
    public AccumuloSplitManager(
            AccumuloConnectorId connectorId,
            AccumuloClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        AccumuloTableLayoutHandle layoutHandle = (AccumuloTableLayoutHandle) layout;
        AccumuloTableHandle tableHandle = layoutHandle.getTable();

        String schemaName = tableHandle.getSchema();
        String tableName = tableHandle.getTable();
        String rowIdName = tableHandle.getRowId();

        // Get non-row ID column constraints
        List<AccumuloColumnConstraint> constraints = getColumnConstraints(rowIdName, layoutHandle.getConstraint());

        // Get the row domain column range
        Optional<Domain> rDom = getRangeDomain(rowIdName, layoutHandle.getConstraint());

        // Call out to our client to retrieve all tablet split metadata using the row ID domain and the secondary index
        List<TabletSplitMetadata> tabletSplits = client.getTabletSplits(session, schemaName, tableName, rDom, constraints, tableHandle.getSerializerInstance());

        // Pack the tablet split metadata into a connector split
        ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
        for (TabletSplitMetadata splitMetadata : tabletSplits) {
            AccumuloSplit split = new AccumuloSplit(
                    connectorId,
                    schemaName,
                    tableName,
                    rowIdName,
                    tableHandle.getSerializerClassName(),
                    splitMetadata.getRanges().stream().map(WrappedRange::new).collect(Collectors.toList()),
                    constraints,
                    tableHandle.getScanAuthorizations(),
                    splitMetadata.getHostPort());
            cSplits.add(split);
        }

        return new FixedSplitSource(cSplits.build());
    }

    private static Optional<Domain> getRangeDomain(String rowIdName, TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.getColumnDomains().isPresent()) {
            for (ColumnDomain<ColumnHandle> cd : constraint.getColumnDomains().get()) {
                AccumuloColumnHandle col = (AccumuloColumnHandle) cd.getColumn();
                if (col.getName().equals(rowIdName)) {
                    return Optional.of(cd.getDomain());
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Gets a list of {@link AccumuloColumnConstraint} based on the given constraint ID, excluding the row ID column
     *
     * @param rowIdName Presto column name mapping to the Accumulo row ID
     * @param constraint Set of query constraints
     * @return List of all column constraints
     */
    private static List<AccumuloColumnConstraint> getColumnConstraints(String rowIdName, TupleDomain<ColumnHandle> constraint)
    {
        ImmutableList.Builder<AccumuloColumnConstraint> constraintBuilder = ImmutableList.builder();
        for (ColumnDomain<ColumnHandle> columnDomain : constraint.getColumnDomains().get()) {
            AccumuloColumnHandle columnHandle = (AccumuloColumnHandle) columnDomain.getColumn();

            if (!columnHandle.getName().equals(rowIdName)) {
                // Family and qualifier will exist for non-row ID columns
                constraintBuilder.add(new AccumuloColumnConstraint(
                        columnHandle.getName(),
                        columnHandle.getFamily().get(),
                        columnHandle.getQualifier().get(),
                        Optional.of(columnDomain.getDomain()),
                        columnHandle.isIndexed()));
            }
        }

        return constraintBuilder.build();
    }
}
