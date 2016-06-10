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
import com.google.common.base.MoreObjects;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

final class WindowMatcher
        implements Matcher
{
    private final List<FunctionCall> functionCalls;

    WindowMatcher(List<FunctionCall> functionCalls)
    {
        this.functionCalls = requireNonNull(functionCalls, "functionCalls is null");
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof WindowNode)) {
            return false;
        }

        WindowNode windowNode = (WindowNode) node;
        Collection<FunctionCall> actualCalls = windowNode.getWindowFunctions().values();

        if (actualCalls.size() != functionCalls.size()) {
            return false;
        }

        LinkedList<FunctionCall> expectedCalls = new LinkedList<>(functionCalls);

        actualCalls:
        for (FunctionCall actualCall : actualCalls) {
            for (int i = 0; i < expectedCalls.size(); ++i) {
                FunctionCall expectedCall = expectedCalls.get(i);
                if (sameCall(expectedCall, actualCall)) {
                    expectedCalls.remove(i);
                    continue actualCalls;
                }
            }
            // Found an actualCall not in expectedCalls.
            return false;
        }

        // actualCalls was missing something in expectedCalls.
        return expectedCalls.isEmpty();
    }

    private static boolean sameCall(FunctionCall left, FunctionCall right)
    {
        return left.getName().equals(right.getName()) &&
                left.getArguments().equals(right.getArguments());
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("functionCalls", functionCalls)
                .toString();
    }
}
