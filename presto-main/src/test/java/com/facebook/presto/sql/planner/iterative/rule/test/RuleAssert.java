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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlan;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.fail;

public class RuleAssert
{
    private final Metadata metadata;
    private final Session session;
    private final Rule rule;

    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    private Map<Symbol, Type> symbols;
    private PlanNode plan;

    public RuleAssert(Metadata metadata, Session session, Rule rule)
    {
        this.metadata = metadata;
        this.session = session;
        this.rule = rule;
    }

    public RuleAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        checkArgument(plan == null, "plan has already been set");

        PlanBuilder builder = new PlanBuilder(idAllocator);
        plan = planProvider.apply(builder);
        symbols = builder.getSymbols();
        return this;
    }

    public void doesNotFire()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator(symbols);
        Optional<PlanNode> result = rule.apply(plan, x -> x, idAllocator, symbolAllocator);

        if (result.isPresent()) {
            fail(String.format(
                    "Expected %s to not fire for:\n%s",
                    rule.getClass().getName(),
                    PlanPrinter.textLogicalPlan(plan, symbolAllocator.getTypes(), metadata, session, 2)));
        }
    }

    public void matches(PlanMatchPattern pattern)
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator(symbols);
        Optional<PlanNode> result = rule.apply(plan, x -> x, idAllocator, symbolAllocator);

        if (!result.isPresent()) {
            fail(String.format(
                    "%s did not fire for:\n%s",
                    rule.getClass().getName(),
                    PlanPrinter.textLogicalPlan(plan, symbolAllocator.getTypes(), metadata, session, 2)));
        }

        PlanNode actual = result.get();

        if (actual == plan) { // plans are not comparable, so we can only ensure they are not the same instance
            fail(String.format(
                    "%s: rule fired but return the original plan:\n%s",
                    rule.getClass().getName(),
                    PlanPrinter.textLogicalPlan(plan, symbolAllocator.getTypes(), metadata, session, 2)));
        }

        if (!ImmutableSet.copyOf(plan.getOutputSymbols()).equals(ImmutableSet.copyOf(actual.getOutputSymbols()))) {
            fail(String.format(
                    "%s: output schema of transformed and original plans are not equivalent\n" +
                            "\texpected: %s\n" +
                            "\tactual:   %s",
                    rule.getClass().getName(),
                    plan.getOutputSymbols(),
                    actual.getOutputSymbols()));
        }

        assertPlan(session, metadata, new Plan(actual, symbolAllocator), pattern);
    }
}
