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
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.Util.domainsMatch;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toMap;

final class TableScanMatcher
        implements Matcher
{
    private final String expectedTableName;
    private final Optional<Map<String, Domain>> expectedConstraint;
    private final Optional<Expression> expectedOriginalConstraint;

    TableScanMatcher(String expectedTableName)
    {
        this(expectedTableName, empty(), empty());
    }

    public TableScanMatcher(String expectedTableName, Map<String, Domain> expectedConstraint)
    {
        this(expectedTableName, Optional.of(expectedConstraint), empty());
    }

    public TableScanMatcher(String expectedTableName, Expression originalConstraint)
    {
        this(expectedTableName, empty(), Optional.of(originalConstraint));
    }

    private TableScanMatcher(String expectedTableName, Optional<Map<String, Domain>> expectedConstraint, Optional<Expression> originalConstraint)
    {
        this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
        this.expectedConstraint = requireNonNull(expectedConstraint, "expectedConstraint is null");
        this.expectedOriginalConstraint = requireNonNull(originalConstraint, "expectedOriginalConstraint is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableScanNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeCost cost, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableScanNode tableScanNode = (TableScanNode) node;
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableScanNode.getTable());
        String actualTableName = tableMetadata.getTable().getTableName();
        return new MatchResult(
                expectedTableName.equalsIgnoreCase(actualTableName) &&
                        originalConstraintMatches(tableScanNode) &&
                        ((!expectedConstraint.isPresent()) ||
                                domainsMatch(expectedConstraint, tableScanNode.getCurrentConstraint(), tableScanNode.getTable(), session, metadata)));
    }

    private boolean originalConstraintMatches(TableScanNode node)
    {
        return expectedOriginalConstraint
                .map(expected -> {
                    Map<String, SymbolReference> assignments = node.getOutputSymbols().stream()
                            .collect(toMap(Symbol::getName, Symbol::toSymbolReference));
                    SymbolAliases symbolAliases = SymbolAliases.builder().putAll(assignments).build();
                    ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
                    return verifier.process(node.getOriginalConstraint(), expected);
                })
                .orElse(true);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("expectedTableName", expectedTableName)
                .add("expectedConstraint", expectedConstraint.orElse(null))
                .add("expectedOriginalConstraint", expectedOriginalConstraint.orElse(null))
                .toString();
    }
}
