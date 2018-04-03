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
package com.facebook.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.connector.ConnectorTableLayoutProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.TableLayout.fromConnectorLayout;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TableLayoutProvider
{
    private final ConnectorTableLayoutProvider layoutProvider;
    private final ConnectorTransactionHandle transaction;
    private final ConnectorId connectorId;

    private Optional<Constraint<ColumnHandle>> constraint = Optional.empty();
    private Optional<Set<ColumnHandle>> desiredColumns = Optional.empty();

    public TableLayoutProvider(ConnectorTableLayoutProvider layoutProvider, ConnectorTransactionHandle transaction, ConnectorId connectorId)
    {
        this.layoutProvider = requireNonNull(layoutProvider, "layoutProvider is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    public List<TableLayoutResult> provide(Session session)
    {
        if (constraint.isPresent()) {
            if (constraint.get().getSummary().isNone()) {
                return ImmutableList.of();
            }

            layoutProvider.getPredicatePushdown().get().pushDownPredicate(constraint.get());
        }

        if (desiredColumns.isPresent()) {
            layoutProvider.getProjectionPushdown().get().pushDownProjection(desiredColumns.get());

        }
        return layoutProvider.provide(session.toConnectorSession(connectorId)).stream()
                .map(layout -> new TableLayoutResult(fromConnectorLayout(connectorId, transaction, layout.getTableLayout()), layout.getUnenforcedConstraint()))
                .collect(toImmutableList());
    }

    public Optional<PredicatePushdown> getPredicatePushdown()
    {
        return layoutProvider.getPredicatePushdown().map(connectorPredicatePushdown -> new PredicatePushdown());
    }

    public Optional<ProjectionPushdown> getProjectionPushdown()
    {
        return layoutProvider.getProjectionPushdown().map(connectorProjectionPushdown -> new ProjectionPushdown());
    }

    public class PredicatePushdown
    {
        public void pushDownPredicate(Constraint<ColumnHandle> constraint)
        {
            if (TableLayoutProvider.this.constraint.isPresent()) {
                TableLayoutProvider.this.constraint = Optional.of(new Constraint<>(
                        constraint.getSummary().intersect(TableLayoutProvider.this.constraint.get().getSummary()),
                        bindings -> TableLayoutProvider.this.constraint.get().predicate().test(bindings) && constraint.predicate().test(bindings)));
            }
            else {
                TableLayoutProvider.this.constraint = Optional.of(constraint);
            }
        }
    }

    public class ProjectionPushdown
    {
        public void pushDownProjection(Set<ColumnHandle> desiredColumns)
        {
            TableLayoutProvider.this.desiredColumns = Optional.of(desiredColumns);
        }
    }
}
