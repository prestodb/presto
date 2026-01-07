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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.ExchangeNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;

import static com.google.common.base.Preconditions.checkArgument;

public class VerifyLocalExchange
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        planNode.accept(new VerifyLocalExchange.Visitor(), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            if (node.getOrderingScheme().isPresent()) {
                checkArgument(!node.getScope().isLocal() || node.getPartitioningScheme().getPartitioning().getHandle().equals(SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION), "local merging exchange requires passthrough distribution");
            }
            return null;
        }
    }
}
