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
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.Util.domainsMatch;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class IndexSourceMatcher
        implements Matcher
{
    private final String expectedTableName;
    private final Optional<Map<String, Domain>> expectedConstraint;

    public IndexSourceMatcher(String expectedTableName)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        expectedConstraint = Optional.empty();
    }

    public IndexSourceMatcher(String expectedTableName, Map<String, Domain> expectedConstraint)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        this.expectedConstraint = Optional.of(ImmutableMap.copyOf(expectedConstraint));
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof IndexSourceNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        IndexSourceNode indexSourceNode = (IndexSourceNode) node;
        TableMetadata tableMetadata = metadata.getTableMetadata(session, indexSourceNode.getTableHandle());
        String actualTableName = tableMetadata.getTable().getTableName();

        if (!expectedTableName.equalsIgnoreCase(actualTableName)) {
            return NO_MATCH;
        }

        if (expectedConstraint.isPresent() &&
                !domainsMatch(
                        expectedConstraint,
                        indexSourceNode.getCurrentConstraint(),
                        indexSourceNode.getTableHandle(),
                        session,
                        metadata)) {
            return NO_MATCH;
        }

        return match();
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
