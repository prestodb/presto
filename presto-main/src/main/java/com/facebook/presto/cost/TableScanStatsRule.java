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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.Objects.requireNonNull;

public class TableScanStatsRule
        extends SimpleStatsRule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    private final Metadata metadata;

    public TableScanStatsRule(Metadata metadata, StatsNormalizer normalizer)
    {
        super(normalizer); // Use stats normalization since connector can return inconsistent stats values
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TableScanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        // TODO Construct predicate like AddExchanges's LayoutConstraintEvaluator
        Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint(), bindings -> true);

        TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
        Map<Symbol, SymbolStatsEstimate> outputSymbolStats = new HashMap<>();

        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            Type symbolType = types.get(symbol);
            Optional<ColumnStatistics> columnStatistics = Optional.ofNullable(tableStatistics.getColumnStatistics().get(entry.getValue()));
            outputSymbolStats.put(symbol, columnStatistics.map(statistics -> toSymbolStatistics(tableStatistics, statistics, session, symbolType)).orElse(UNKNOWN_STATS));
        }

        return Optional.of(PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue())
                .addSymbolStatistics(outputSymbolStats)
                .build());
    }

    private SymbolStatsEstimate toSymbolStatistics(TableStatistics tableStatistics, ColumnStatistics columnStatistics, Session session, Type type)
    {
        double nullsFraction = columnStatistics.getNullsFraction().getValue();
        double nonNullRowsCount = tableStatistics.getRowCount().getValue() * (1.0 - nullsFraction);
        return SymbolStatsEstimate.builder()
                .setLowValue(asDouble(session, type, columnStatistics.getOnlyRangeColumnStatistics().getLowValue()).orElse(NEGATIVE_INFINITY))
                .setHighValue(asDouble(session, type, columnStatistics.getOnlyRangeColumnStatistics().getHighValue()).orElse(POSITIVE_INFINITY))
                .setNullsFraction(nullsFraction)
                .setDistinctValuesCount(columnStatistics.getOnlyRangeColumnStatistics().getDistinctValuesCount().getValue())
                .setAverageRowSize(columnStatistics.getOnlyRangeColumnStatistics().getDataSize().getValue() / nonNullRowsCount)
                .build();
    }

    private OptionalDouble asDouble(Session session, Type type, Optional<Object> optionalValue)
    {
        return optionalValue
                .map(value -> toStatsRepresentation(metadata, session, type, value))
                .orElseGet(OptionalDouble::empty);
    }
}
