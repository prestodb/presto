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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyMap;

public class TestScalarStatsCalculator
{
    private ScalarStatsCalculator calculator;
    private Session session;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        calculator = new ScalarStatsCalculator(MetadataManager.createTestMetadataManager());
        session = testSessionBuilder().build();
    }

    @Test
    public void testLiteral()
    {
        assertCalculate(new DoubleLiteral("7.5"))
                .distinctValuesCount(1.0)
                .lowValue(7.5)
                .highValue(7.5)
                .nullsFraction(0.0);

        assertCalculate(new DecimalLiteral("75.5"))
                .distinctValuesCount(1.0)
                .lowValue(75.5)
                .highValue(75.5)
                .nullsFraction(0.0);

        assertCalculate(new StringLiteral("blah"))
                .distinctValuesCount(1.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(0.0);

        assertCalculate(new NullLiteral())
                .distinctValuesCount(0.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(1.0);
    }

    private SymbolStatsAssertion assertCalculate(Expression scalarExpression)
    {
        return assertCalculate(scalarExpression, UNKNOWN_STATS);
    }

    private SymbolStatsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics)
    {
        return assertCalculate(scalarExpression, inputStatistics, emptyMap());
    }

    private SymbolStatsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Map<Symbol, Type> types)
    {
        return SymbolStatsAssertion.assertThat(calculator.calculate(scalarExpression, inputStatistics, session, types));
    }
}
