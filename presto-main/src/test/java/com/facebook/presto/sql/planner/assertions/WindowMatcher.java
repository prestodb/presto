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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Optionally validates each of the non-function fields of the node.
 */
public final class WindowMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<WindowNode.Specification>> specification;

    private WindowMatcher(Optional<ExpectedValueProvider<WindowNode.Specification>> specification)
    {
        this.specification = requireNonNull(specification, "specification is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof WindowNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeCost cost, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        WindowNode windowNode = (WindowNode) node;

        if (!specification
                .map(expectedSpecification ->
                        expectedSpecification.getExpectedValue(symbolAliases)
                        .equals(windowNode.getSpecification()))
                .orElse(true)) {
            return NO_MATCH;
        }

        /*
         * Window functions produce a symbol (the result of the function call) that we might
         * want to bind to an alias so we can reference it further up the tree. As such,
         * they need to be matched with an Alias matcher so we can bind the symbol if desired.
         */
        return match();
    }

    @Override
    public String toString()
    {
        // Only include fields in the description if they are actual constraints.
        return toStringHelper(this)
                .omitNullValues()
                .add("specification", specification.orElse(null))
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<ExpectedValueProvider<WindowNode.Specification>> specification = Optional.empty();
        private List<AliasMatcher> windowFunctionMatchers = new LinkedList<>();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder specification(
                List<String> partitionBy,
                List<String> orderBy,
                Map<String, SortOrder> orderings)
        {
            return specification(PlanMatchPattern.specification(partitionBy, orderBy, orderings));
        }

        public Builder specification(ExpectedValueProvider<WindowNode.Specification> specification)
        {
            this.specification = Optional.of(specification);
            return this;
        }

        public Builder addFunction(String output, ExpectedValueProvider<FunctionCall> functionCall)
        {
            return addFunction(Optional.of(output), functionCall);
        }

        public Builder addFunction(ExpectedValueProvider<FunctionCall> functionCall)
        {
            return addFunction(Optional.empty(), functionCall);
        }

        private Builder addFunction(Optional<String> output, ExpectedValueProvider<FunctionCall> functionCall)
        {
            windowFunctionMatchers.add(new AliasMatcher(output, new WindowFunctionMatcher(functionCall, Optional.empty(), Optional.empty())));
            return this;
        }

        PlanMatchPattern build()
        {
            PlanMatchPattern result = node(WindowNode.class, source).with(
                    new WindowMatcher(specification));
            windowFunctionMatchers.forEach(result::with);
            return result;
        }
    }
}
