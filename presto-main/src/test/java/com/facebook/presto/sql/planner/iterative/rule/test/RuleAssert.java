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
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlan;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

public class RuleAssert
{
    private final Metadata metadata;
    private Session session;
    private final Rule rule;

    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    private Map<Symbol, Type> symbols;
    private PlanNode plan;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    public RuleAssert(Metadata metadata, Session session, Rule rule, TransactionManager transactionManager, AccessControl accessControl, StatsCalculator statsCalculator, CostCalculator costCalculator)
    {
        this.metadata = metadata;
        this.session = session;
        this.rule = rule;
        this.transactionManager = transactionManager;
        this.accessControl = accessControl;
        this.statsCalculator = statsCalculator;
        this.costCalculator = costCalculator;
    }

    public RuleAssert setSystemProperty(String key, String value)
    {
        return withSession(Session.builder(session)
                .setSystemProperty(key, value)
                .build());
    }

    public RuleAssert withSession(Session session)
    {
        this.session = session;
        return this;
    }

    public RuleAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        checkArgument(plan == null, "plan has already been set");

        PlanBuilder builder = new PlanBuilder(idAllocator, metadata);
        plan = planProvider.apply(builder);
        symbols = builder.getSymbols();
        return this;
    }

    public void doesNotFire()
    {
        RuleApplication ruleApplication = applyRule();

        if (ruleApplication.wasRuleApplied()) {
            fail(String.format(
                    "Expected %s to not fire for:\n%s",
                    rule.getClass().getName(),
                    inTransaction(session -> PlanPrinter.textLogicalPlan(plan, ruleApplication.types, metadata, ruleApplication.lookup, session, 2))));
        }
    }

    public void matches(PlanMatchPattern pattern)
    {
      RuleApplication ruleApplication = applyRule();
      Map<Symbol, Type> types = ruleApplication.types;

      if (!ruleApplication.wasRuleApplied()) {
          fail(String.format(
                  "%s did not fire for:\n%s",
                  rule.getClass().getName(),
                  formatPlan(plan, types, ruleApplication.lookup)));
      }

      PlanNode actual = ruleApplication.getResult();

      if (actual == plan) { // plans are not comparable, so we can only ensure they are not the same instance
          fail(String.format(
                  "%s: rule fired but return the original plan:\n%s",
                  rule.getClass().getName(),
                  formatPlan(plan, types, ruleApplication.lookup)));
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

      inTransaction(session -> {
          assertPlan(session, metadata, ruleApplication.lookup, new Plan(actual, types, ruleApplication.lookup, session), pattern);
          return null;
      });
  }

  private RuleApplication applyRule()
  {
      SymbolAllocator symbolAllocator = new SymbolAllocator(symbols);
      Memo memo = new Memo(idAllocator, plan);
      Lookup lookup = Lookup.from(memo::resolve, statsCalculator, costCalculator);

      if (!rule.getPattern().matches(plan)) {
          return new RuleApplication(lookup, symbolAllocator.getTypes(), Optional.empty());
      }

      Optional<PlanNode> result = inTransaction(session -> rule.apply(memo.getNode(memo.getRootGroup()), lookup, idAllocator, symbolAllocator, session));

      return new RuleApplication(lookup, symbolAllocator.getTypes(), result);
  }

  private String formatPlan(PlanNode plan, Map<Symbol, Type> types, Lookup lookup)
  {
      return inTransaction(session -> PlanPrinter.textLogicalPlan(plan, types, metadata, lookup, session, 2));
  }

  private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
  {
      return transaction(transactionManager, accessControl)
              .singleStatement()
              .execute(session, session -> {
                  // metadata.getCatalogHandle() registers the catalog for the transaction
                  session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                  return transactionSessionConsumer.apply(session);
              });
  }

  private static class RuleApplication
  {
      private final Lookup lookup;
      private final Map<Symbol, Type> types;
      private final Optional<PlanNode> result;

      public RuleApplication(Lookup lookup, Map<Symbol, Type> types, Optional<PlanNode> result)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.types = requireNonNull(types, "types is null");
            this.result = requireNonNull(result, "result is null");
        }

        private boolean wasRuleApplied()
        {
            return result.isPresent();
        }

        public PlanNode getResult()
        {
            return result.orElseThrow(() -> new IllegalStateException("Rule was not applied"));
        }
    }
}
