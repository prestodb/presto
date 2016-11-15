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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

final class WindowMatcher
        implements Matcher
{
    private final ExpectedValueProvider<WindowNode.Specification> specification;

    WindowMatcher(
            ExpectedValueProvider<WindowNode.Specification> specification)
    {
        this.specification = specification;
    }

    @Override
    public boolean downMatches(PlanNode node)
    {
        return node instanceof WindowNode;
    }

    @Override
    public boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        checkState(downMatches(node), "Plan testing framework error: downMatches returned false in upMatches in %s", this.getClass().getName());

        WindowNode windowNode = (WindowNode) node;

        /*
         * Window functions produce a symbol (the result of the function call) that we might
         * want to bind to an alias so we can reference it further up the tree. As such,
         * they need to be matched with an Alias matcher so we can bind the symbol if desired.
         */
        return windowNode.getSpecification().equals(specification.getExpectedValue(expressionAliases));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .toString();
    }
}
