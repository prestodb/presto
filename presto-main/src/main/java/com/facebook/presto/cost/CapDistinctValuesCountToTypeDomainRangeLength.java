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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Map;

import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.Math.floor;
import static java.lang.Math.pow;

public class CapDistinctValuesCountToTypeDomainRangeLength
        implements ComposableStatsCalculator.Normalizer
{
    @Override
    public PlanNodeStatsEstimate normalize(PlanNode node, PlanNodeStatsEstimate estimate, Map<Symbol, Type> types)
    {
        for (Symbol symbol : estimate.getSymbolsWithKnownStatistics()) {
            double domainLength = calculateDomainLength(symbol, estimate, types);
            if (isNaN(domainLength)) {
                continue;
            }
            estimate = estimate.mapSymbolColumnStatistics(
                    symbol,
                    symbolStatsEstimate -> symbolStatsEstimate.mapDistinctValuesCount(
                            distinctValuesCount -> min(distinctValuesCount, domainLength)
                    ));
        }

        return estimate;
    }

    private double calculateDomainLength(Symbol symbol, PlanNodeStatsEstimate estimate, Map<Symbol, Type> types)
    {
        SymbolStatsEstimate symbolStatistics = estimate.getSymbolStatistics(symbol);

        if (symbolStatistics.statisticRange().length() == 0) {
            return 1;
        }

        Type type = types.get(symbol);
        if (!isDiscrete(type)) {
            return NaN;
        }

        double length = symbolStatistics.getHighValue() - symbolStatistics.getLowValue();
        if (type instanceof DecimalType) {
            length *= pow(10, ((DecimalType) type).getScale());
        }
        return floor(length + 1);
    }

    private boolean isDiscrete(Type type)
    {
        return type.equals(IntegerType.INTEGER) ||
                type.equals(BigintType.BIGINT) ||
                type.equals(SmallintType.SMALLINT) ||
                type.equals(TinyintType.TINYINT) ||
                type.equals(BooleanType.BOOLEAN) ||
                type instanceof DecimalType;
    }
}
