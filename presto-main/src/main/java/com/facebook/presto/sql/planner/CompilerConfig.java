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

import com.facebook.presto.spi.function.Description;
import io.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class CompilerConfig
{
    private boolean interpreterEnabled;
    private int expressionCacheSize = 10_000;

    public boolean isInterpreterEnabled()
    {
        return interpreterEnabled;
    }

    @Config("compiler.interpreter-enabled")
    @Description("Allows evaluation to fall back to interpreter if compilation fails")
    public CompilerConfig setInterpreterEnabled(boolean interpreterEnabled)
    {
        this.interpreterEnabled = interpreterEnabled;
        return this;
    }

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
}
