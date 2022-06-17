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
package com.facebook.presto.sql.planner;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class CollectSourceStats
{
    private static final Logger log = Logger.get(CollectSourceStats.class);
    private final Metadata metadata;
    private final Session session;

    public CollectSourceStats(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public Pair<Double, Boolean> collectSourceStats(PlanNode root)
    {
        log.info("Sourav inside collectSourceStats");
        Visitor visitor = new Visitor();
        root.accept(visitor, null);

        Pair<List<TableStatistics>, Boolean> sourceStatistics = visitor.getStatsForSources();

        Iterator<TableStatistics> tableStatisticsIterator = sourceStatistics.getKey().iterator();
        Double totalSourceSize = 0.0;
        while (tableStatisticsIterator.hasNext()) {
            TableStatistics ts = tableStatisticsIterator.next();
            totalSourceSize = totalSourceSize + ts.getTotalSize().getValue();
        }

        return new Pair<Double, Boolean>(totalSourceSize, sourceStatistics.getValue());
    }

    private class Visitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final ImmutableList.Builder<TableStatistics> tableStatisticsBuilder = ImmutableList.builder();
        private Boolean isSourceMissingData = false;

        public Pair<List<TableStatistics>, Boolean> getStatsForSources()
        {
            return new Pair<>(tableStatisticsBuilder.build(), isSourceMissingData);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            Optional<TableStatistics> statistics = Optional.empty();
            Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());
            statistics = Optional.ofNullable(metadata.getTableStatistics(session, node.getTable(), ImmutableList.of(), constraint));

            if (statistics.isPresent()) {
                tableStatisticsBuilder.add(statistics.get());
            }
            else {
                isSourceMissingData = true;
            }

            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode source : node.getSources()) {
                log.info("iterating over source node->" + source.toString());
                source.accept(this, context);
            }
            return null;
        }
    }

    @Immutable
    public static class Pair<K, V>
    {
        private final K key;
        private final V value;

        @JsonCreator
        public Pair(@JsonProperty("key") K key, @JsonProperty("value") V value)
        {
            this.key = requireNonNull(key, "key is null");
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty
        public K getKey()
        {
            return key;
        }

        @JsonProperty
        public V getValue()
        {
            return value;
        }
    }
}
