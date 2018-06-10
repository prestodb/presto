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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter.rewrite;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestLambdaCaptureDesugaringRewriter
{
    @Test
    public void testRewriteBasicLambda()
    {
        final Map<Symbol, Type> symbols = ImmutableMap.of(new Symbol("a"), BigintType.BIGINT);
        final SymbolAllocator allocator = new SymbolAllocator(symbols);

        assertEquals(rewrite(expression("x -> a + x"), allocator.getTypes(), allocator),
                     new BindExpression(
                             ImmutableList.of(expression("a")),
                             new LambdaExpression(
                                     Stream.of("a_0", "x")
                                             .map(Identifier::new)
                                             .map(LambdaArgumentDeclaration::new)
                                             .collect(toList()),
                                     expression("a_0 + x"))));
    }
}
