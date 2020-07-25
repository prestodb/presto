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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.Util.domainsMatch;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class TableScanMatcher
        implements Matcher
{
    private final String expectedTableName;
    private final Optional<Map<String, Domain>> expectedConstraint;
    private final Optional<Boolean> hasTableLayout;

    private TableScanMatcher(String expectedTableName, Optional<Map<String, Domain>> expectedConstraint, Optional<Boolean> hasTableLayout)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        this.expectedConstraint = requireNonNull(expectedConstraint, "expectedConstraint is null");
        this.hasTableLayout = requireNonNull(hasTableLayout, "hasTableLayout is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableScanNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableScanNode tableScanNode = (TableScanNode) node;
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableScanNode.getTable());
        String actualTableName = tableMetadata.getTable().getTableName();
        return new MatchResult(
                expectedTableName.equalsIgnoreCase(actualTableName) &&
                        ((!expectedConstraint.isPresent()) ||
                                domainsMatch(expectedConstraint, tableScanNode.getCurrentConstraint(), tableScanNode.getTable(), session, metadata)));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("expectedTableName", expectedTableName)
                .add("expectedConstraint", expectedConstraint.orElse(null))
                .add("hasTableLayout", hasTableLayout.orElse(null))
                .toString();
    }

    public static Builder builder(String expectedTableName)
    {
        return new Builder(expectedTableName);
    }

    public static PlanMatchPattern create(String expectedTableName)
    {
        return builder(expectedTableName).build();
    }

    public static class Builder
    {
        private final String expectedTableName;
        private Optional<Map<String, Domain>> expectedConstraint = Optional.empty();
        private Optional<Boolean> hasTableLayout = Optional.empty();

        private Builder(String expectedTableName)
        {
            this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        }

        public Builder expectedConstraint(Map<String, Domain> expectedConstraint)
        {
            this.expectedConstraint = Optional.of(expectedConstraint);
            return this;
        }

        public Builder hasTableLayout()
        {
            this.hasTableLayout = Optional.of(true);
            return this;
        }

        PlanMatchPattern build()
        {
            PlanMatchPattern result = node(TableScanNode.class).with(
                    new TableScanMatcher(
                            expectedTableName,
                            expectedConstraint,
                            hasTableLayout));
            return result;
        }
    }
}
