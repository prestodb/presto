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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLambdaCaptureDesugaringRowExpressionRewriter
{
    private final TestingRowExpressionTranslator testSqlToRowExpressionTranslator = new TestingRowExpressionTranslator();

    @Test
    public void testRewriteBasicLambda()
    {
        RowExpression inputExpression = testSqlToRowExpressionTranslator.translate("any_match(ARRAY[], x -> x = a)", ImmutableMap.of("a", BIGINT));
        CallExpression rewritten = (CallExpression) LambdaCaptureDesugaringRowExpressionRewriter.rewrite(inputExpression, new VariableAllocator(ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "a", BIGINT))));
        assertTrue(rewritten.getArguments().get(1) instanceof SpecialFormExpression);
        SpecialFormExpression bind = (SpecialFormExpression) rewritten.getArguments().get(1);
        assertEquals(bind.toString(), "BIND(a, (a_0,x) -> EQUAL(x, a_0))");
    }
}
