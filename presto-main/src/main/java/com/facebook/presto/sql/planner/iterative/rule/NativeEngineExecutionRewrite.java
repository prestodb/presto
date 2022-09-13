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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.NativeEngineNode;

/**
 * Encapsulate the plan fragment into NativeEngine Node which will be serialized to external native engine process for execution.
 */
public class NativeEngineExecutionRewrite
        implements Rule<PlanNode>
{
    // private static final Pattern PATTERN = Pattern.any();
    private static final Pattern PATTERN = Pattern.any().matching(node -> !(node instanceof NativeEngineNode));

    @Override
    public Pattern<PlanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return false;
        // return session.getSystemProperty("spark_native_engine_execution_enabled", Boolean.class);
    }

    @Override
    public Result apply(PlanNode node, Captures captures, Context context)
    {
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        Session session = context.getSession();
        return Result.ofPlanNode(new NativeEngineNode(node.getSourceLocation(), idAllocator.getNextId(), node));
    }
}
