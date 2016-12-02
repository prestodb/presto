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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class TableScanMatcher
        implements Matcher
{
    private final String expectedTableName;
    private final Optional<Map<String, Domain>> expectedConstraint;

    TableScanMatcher(String expectedTableName)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        expectedConstraint = Optional.empty();
    }

    public TableScanMatcher(String expectedTableName, Map<String, Domain> expectedConstraint)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        this.expectedConstraint = Optional.of(requireNonNull(expectedConstraint, "expectedConstraint is null"));
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableScanNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableScanNode tableScanNode = (TableScanNode) node;
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableScanNode.getTable());
        String actualTableName = tableMetadata.getTable().getTableName();
        return new MatchResult(
                expectedTableName.equalsIgnoreCase(actualTableName) &&
                domainMatches(tableScanNode, session, metadata));
    }

    private boolean domainMatches(TableScanNode tableScanNode, Session session, Metadata metadata)
    {
        if (!expectedConstraint.isPresent()) {
            return true;
        }

        TupleDomain<ColumnHandle> actualConstraint = tableScanNode.getCurrentConstraint();
        if (expectedConstraint.isPresent() && !actualConstraint.getDomains().isPresent()) {
            return false;
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableScanNode.getTable());
        for (Map.Entry<String, Domain> expectedColumnConstraint : expectedConstraint.get().entrySet()) {
            if (!columnHandles.containsKey(expectedColumnConstraint.getKey())) {
                return false;
            }
            ColumnHandle columnHandle = columnHandles.get(expectedColumnConstraint.getKey());
            if (!actualConstraint.getDomains().get().containsKey(columnHandle)) {
                return false;
            }
            if (!expectedColumnConstraint.getValue().contains(actualConstraint.getDomains().get().get(columnHandle))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("expectedTableName", expectedTableName)
                .add("expectedConstraint", expectedConstraint.orElse(null))
                .toString();
    }
}
