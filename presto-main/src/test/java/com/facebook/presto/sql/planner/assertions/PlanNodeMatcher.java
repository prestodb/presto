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
import com.google.common.base.MoreObjects;

import static java.util.Objects.requireNonNull;

final class PlanNodeMatcher
        implements Matcher
{
    private final Class<? extends PlanNode> nodeClass;

    public PlanNodeMatcher(Class<? extends PlanNode> nodeClass)
    {
        this.nodeClass = requireNonNull(nodeClass, "nodeClass is null");
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return node.getClass().equals(nodeClass);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("nodeClass", nodeClass)
                .toString();
    }
}
