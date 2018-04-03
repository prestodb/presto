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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LegacyConnectorTableLayoutProvider
        implements ConnectorTableLayoutProvider
{
    private final ConnectorMetadata connectorMetadata;
    private final ConnectorSession session;
    private final ConnectorTableHandle table;

    private Constraint<ColumnHandle> constraint = Constraint.alwaysTrue();
    private Optional<Set<ColumnHandle>> desiredColumns = Optional.empty();

    public LegacyConnectorTableLayoutProvider(ConnectorMetadata connectorMetadata, ConnectorSession session, ConnectorTableHandle table)
    {
        this.connectorMetadata = connectorMetadata;
        this.session = session;
        this.table = table;
    }

    @Override
    public List<ConnectorTableLayoutResult> provide(ConnectorSession session)
    {
        return connectorMetadata.getTableLayouts(this.session, table, constraint, desiredColumns);
    }

    @Override
    public Optional<PredicatePushdown> getPredicatePushdown()
    {
        return Optional.of(constraint -> LegacyConnectorTableLayoutProvider.this.constraint = constraint);
    }

    @Override
    public Optional<ProjectionPushdown> getProjectionPushdown()
    {
        return Optional.of(desiredColumns -> LegacyConnectorTableLayoutProvider.this.desiredColumns = Optional.of(desiredColumns));
    }
}
