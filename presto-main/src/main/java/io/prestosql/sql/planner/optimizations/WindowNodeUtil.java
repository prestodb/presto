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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.plan.WindowNode;

import java.util.Collection;

public final class WindowNodeUtil
{
    private WindowNodeUtil() {}

    public static boolean dependsOn(WindowNode parent, WindowNode child)
    {
        return parent.getPartitionBy().stream().anyMatch(child.getCreatedSymbols()::contains)
                || (parent.getOrderingScheme().isPresent() && parent.getOrderingScheme().get().getOrderBy().stream().anyMatch(child.getCreatedSymbols()::contains))
                || parent.getWindowFunctions().values().stream()
                .map(WindowNode.Function::getFunctionCall)
                .map(SymbolsExtractor::extractUnique)
                .flatMap(Collection::stream)
                .anyMatch(child.getCreatedSymbols()::contains);
    }
}
