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
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

final class WindowMatcher
        implements Matcher
{
    private final List<FunctionCall> functionCalls;

    WindowMatcher(List<FunctionCall> functionCalls)
    {
        this.functionCalls = ImmutableList.copyOf(requireNonNull(functionCalls, "functionCalls is null"));
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        if (!(node instanceof WindowNode)) {
            return false;
        }

        WindowNode windowNode = (WindowNode) node;
        LinkedList<FunctionCall> actualCalls = windowNode.getWindowFunctions().values().stream()
                .map(WindowNode.Function::getFunctionCall)
                .collect(Collectors.toCollection(LinkedList::new));

        if (actualCalls.size() != functionCalls.size()) {
            return false;
        }

        for (FunctionCall expectedCall : functionCalls) {
            if (!actualCalls.remove(expectedCall)) {
                // Found an expectedCall not in expectedCalls.
                return false;
            }
        }

        // expectedCalls was missing something in actualCalls.
        return actualCalls.isEmpty();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("functionCalls", functionCalls)
                .toString();
    }
}
