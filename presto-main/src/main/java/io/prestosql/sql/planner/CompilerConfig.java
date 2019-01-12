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
package io.prestosql.sql.planner;

import io.airlift.configuration.Config;
import io.airlift.configuration.DefunctConfig;
import io.prestosql.spi.function.Description;

import javax.validation.constraints.Min;

@DefunctConfig("compiler.interpreter-enabled")
public class CompilerConfig
{
    private int expressionCacheSize = 10_000;

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
