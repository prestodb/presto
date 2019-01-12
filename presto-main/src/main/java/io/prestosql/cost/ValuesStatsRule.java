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
package io.prestosql.cost;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.cost.ComposableStatsCalculator.Rule;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.cost.StatsUtil.toStatsRepresentation;
import static io.prestosql.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static io.prestosql.sql.planner.plan.Patterns.values;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.util.stream.Collectors.toList;

public class ValuesStatsRule
        implements Rule<ValuesNode>
{
    private static final Pattern<ValuesNode> PATTERN = values();

    private final Metadata metadata;

    public ValuesStatsRule(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<ValuesNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(ValuesNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        statsBuilder.setOutputRowCount(node.getRows().size());

        for (int symbolId = 0; symbolId < node.getOutputSymbols().size(); ++symbolId) {
            Symbol symbol = node.getOutputSymbols().get(symbolId);
            List<Object> symbolValues = getSymbolValues(node, symbolId, session, types.get(symbol));
            statsBuilder.addSymbolStatistics(symbol, buildSymbolStatistics(symbolValues, session, types.get(symbol)));
        }

        return Optional.of(statsBuilder.build());
    }

    private List<Object> getSymbolValues(ValuesNode valuesNode, int symbolId, Session session, Type symbolType)
    {
        if (UNKNOWN.equals(symbolType)) {
            // special casing for UNKNOWN as evaluateConstantExpression does not handle that
            return IntStream.range(0, valuesNode.getRows().size())
                    .mapToObj(rowId -> null)
                    .collect(toList());
        }
        return valuesNode.getRows().stream()
                .map(row -> row.get(symbolId))
                .map(expression -> evaluateConstantExpression(expression, symbolType, metadata, session, ImmutableList.of()))
                .collect(toList());
    }

    private SymbolStatsEstimate buildSymbolStatistics(List<Object> values, Session session, Type type)
    {
        List<Object> nonNullValues = values.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (nonNullValues.isEmpty()) {
            return SymbolStatsEstimate.zero();
        }

        double[] valuesAsDoubles = nonNullValues.stream()
                .map(value -> toStatsRepresentation(metadata, session, type, value))
                .filter(OptionalDouble::isPresent)
                .mapToDouble(OptionalDouble::getAsDouble)
                .toArray();

        double lowValue = DoubleStream.of(valuesAsDoubles).min().orElse(Double.NEGATIVE_INFINITY);
        double highValue = DoubleStream.of(valuesAsDoubles).max().orElse(Double.POSITIVE_INFINITY);
        double valuesCount = values.size();
        double nonNullValuesCount = nonNullValues.size();
        long distinctValuesCount = nonNullValues.stream().distinct().count();

        return SymbolStatsEstimate.builder()
                .setNullsFraction((valuesCount - nonNullValuesCount) / valuesCount)
                .setLowValue(lowValue)
                .setHighValue(highValue)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }
}
