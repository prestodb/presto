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

import static java.util.Objects.requireNonNull;

final class PlanMatchingContext
{
    private final SymbolAliases symbolAliases;
    private final PlanMatchPattern pattern;

    PlanMatchingContext(PlanMatchPattern pattern)
    {
        this(new SymbolAliases(), pattern);
    }

    PlanMatchingContext(SymbolAliases symbolAliases, PlanMatchPattern pattern)
    {
        requireNonNull(symbolAliases, "symbolAliases is null");
        requireNonNull(pattern, "pattern is null");
        this.symbolAliases = new SymbolAliases(symbolAliases);
        this.pattern = pattern;
    }

    PlanMatchPattern getPattern()
    {
        return pattern;
    }

    SymbolAliases getSymbolAliases()
    {
        return symbolAliases;
    }
}
