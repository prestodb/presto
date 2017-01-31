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

package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.sql.planner.plan.PlanNode;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public abstract class Pattern
{
    private static final Pattern ANY_NODE = new MatchNodeClass(PlanNode.class);

    private Pattern() {}

    public abstract boolean matches(PlanNode node);

    public static Pattern any()
    {
        return ANY_NODE;
    }

    public static <T extends PlanNode> Pattern node(Class<T> nodeClass)
    {
        return new MatchNodeClass(nodeClass);
    }

    static class MatchNodeClass<T extends PlanNode>
            extends Pattern
    {
        private final Class<T> nodeClass;

        MatchNodeClass(Class<T> nodeClass)
        {
            this.nodeClass = requireNonNull(nodeClass, "nodeClass is null");
        }

        Class<T> getNodeClass()
        {
            return nodeClass;
        }

        @Override
        public boolean matches(PlanNode node)
        {
            return nodeClass.isInstance(node);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("nodeClass", nodeClass)
                    .toString();
        }
    }
}
