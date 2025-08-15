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
package com.facebook.presto.sql.planner;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.facebook.presto.spi.function.Description;

import javax.validation.constraints.Min;

@DefunctConfig("compiler.interpreter-enabled")
public class CompilerConfig
{
    private int expressionCacheSize = 10_000;
    private int leafNodeLimit = 10_000;
    private boolean leafNodeLimitEnabled;

    @Min(0)
    public int getExpressionCacheSize()
    {
        return expressionCacheSize;
    }

    @Config("compiler.expression-cache-size")
    @Description("Reuse compiled expressions across multiple queries")
    public CompilerConfig setExpressionCacheSize(int expressionCacheSize)
    {
        this.expressionCacheSize = expressionCacheSize;
        return this;
    }

    public int getLeafNodeLimit()
    {
        return this.leafNodeLimit;
    }

    @Config("planner.max-leaf-nodes-in-plan")
    @ConfigDescription("Maximum number of leaf nodes in logical plan, throw an exception when exceed if leaf-node-limit-enabled is set true")
    public CompilerConfig setLeafNodeLimit(int num)
    {
        this.leafNodeLimit = num;
        return this;
    }

    public boolean getLeafNodeLimitEnabled()
    {
        return this.leafNodeLimitEnabled;
    }

    @Config("planner.leaf-node-limit-enabled")
    @ConfigDescription("Throw an exception if number of leaf nodes in logical plan exceeds threshold set in max-leaf-nodes-in-plan")
    public CompilerConfig setLeafNodeLimitEnabled(boolean enabled)
    {
        this.leafNodeLimitEnabled = enabled;
        return this;
    }
}
