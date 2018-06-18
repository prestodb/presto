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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.ActualProperties;
import com.facebook.presto.sql.planner.optimizations.PropertyDerivations;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.sanity.PlanSanityChecker.Checker;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.util.Optionals.combine;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * When an aggregation has an empty grouping set then a default value needs to be returned in the output (e.g: 0 for COUNT(*)).
 * In case if the aggregation is split into FINAL and PARTIAL, then default values are produced by PARTIAL
 * aggregations. In order for the default values not to be duplicated, FINAL aggregation needs to be
 * separated from PARTIAL aggregation by a remote repartition exchange or the FINAL aggregation needs to be executed
 * on a single node. In case both FINAL and PARTIAL aggregations are executed on a single node, then those need to separated
 * by a local repartition exchange or the FINAL aggregation needs to be executed in a single thread.
 */
public class ValidateAggregationsWithDefaultValues
        implements Checker
{
    private final boolean forceSingleNode;

    public ValidateAggregationsWithDefaultValues(boolean forceSingleNode)
    {
        this.forceSingleNode = forceSingleNode;
    }

    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
    {
        planNode.accept(new Visitor(session, metadata, sqlParser, types), null);
    }

    private class Visitor
            extends PlanVisitor<Optional<SeenExchanges>, Void>
    {
        final Session session;
        final Metadata metadata;
        final SqlParser parser;
        final TypeProvider types;

        Visitor(Session session, Metadata metadata, SqlParser parser, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.parser = requireNonNull(parser, "parser is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected Optional<SeenExchanges> visitPlan(PlanNode node, Void context)
        {
            return aggregatedSeenExchanges(node.getSources());
        }

        @Override
        public Optional<SeenExchanges> visitAggregation(AggregationNode node, Void context)
        {
            Optional<SeenExchanges> seenExchangesOptional = aggregatedSeenExchanges(node.getSources());

            if (node.getStep().equals(PARTIAL)) {
                return Optional.of(new SeenExchanges(false, false));
            }

            if (node.getStep().equals(INTERMEDIATE)) {
                return seenExchangesOptional;
            }

            // We only validate FINAL aggregations with empty grouping set
            if (!node.getStep().equals(FINAL) || !node.hasEmptyGroupingSet()) {
                return Optional.empty();
            }

            checkState(seenExchangesOptional.isPresent(), "No partial aggregation below final aggregation");
            SeenExchanges seenExchanges = seenExchangesOptional.get();

            if (seenExchanges.remoteRepartitionExchange) {
                // Final aggregation separated from partial by remote repartition exchange.
                return Optional.empty();
            }

            // No remote repartition exchange between final and partial aggregation.
            // Make sure that final aggregation operators are executed on a single node.
            ActualProperties globalProperties = PropertyDerivations.derivePropertiesRecursively(node, metadata, session, types, parser);
            checkArgument(forceSingleNode || globalProperties.isSingleNode(),
                    "Final aggregation with default value not separated from partial aggregation by remote hash exchange");

            if (!seenExchanges.localRepartitionExchange) {
                // No local repartition exchange between final and partial aggregation.
                // Make sure that final aggregation operators are executed by single thread.
                StreamProperties localProperties = StreamPropertyDerivations.derivePropertiesRecursively(node, metadata, session, types, parser);
                checkArgument(localProperties.isSingleStream(),
                        "Final aggregation with default value not separated from partial aggregation by local hash exchange");
            }

            return Optional.empty();
        }

        @Override
        public Optional<SeenExchanges> visitExchange(ExchangeNode node, Void context)
        {
            Optional<SeenExchanges> seenExchangesOptional = aggregatedSeenExchanges(node.getSources());
            if (!seenExchangesOptional.isPresent()) {
                // No partial aggregation below
                return Optional.empty();
            }

            if (!node.getType().equals(REPARTITION)) {
                return seenExchangesOptional;
            }

            SeenExchanges seenExchanges = seenExchangesOptional.get();
            if (node.getScope().equals(REMOTE)) {
                return Optional.of(new SeenExchanges(false, true));
            }

            return Optional.of(new SeenExchanges(true, seenExchanges.remoteRepartitionExchange));
        }

        private Optional<SeenExchanges> aggregatedSeenExchanges(List<PlanNode> nodes)
        {
            return nodes.stream()
                    .map(source -> source.accept(this, null))
                    .reduce((accumulatorOptional, seenExchangesOptional) -> combine(accumulatorOptional, seenExchangesOptional,
                            (accumulator, seenExchanges) -> new SeenExchanges(
                                    accumulator.localRepartitionExchange && seenExchanges.localRepartitionExchange,
                                    accumulator.remoteRepartitionExchange && seenExchanges.remoteRepartitionExchange)))
                    .orElse(Optional.empty());
        }
    }

    private static class SeenExchanges
    {
        final boolean localRepartitionExchange;
        final boolean remoteRepartitionExchange;

        SeenExchanges(boolean localRepartitionExchange, boolean remoteRepartitionExchange)
        {
            this.localRepartitionExchange = localRepartitionExchange;
            this.remoteRepartitionExchange = remoteRepartitionExchange;
        }
    }
}
