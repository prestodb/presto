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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;

import java.util.Collection;

import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.ROWS;
import static java.lang.String.format;

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

    public static WindowType toWindowType(WindowFrame.Type type)
    {
        switch (type) {
            case RANGE:
                return RANGE;
            case ROWS:
                return ROWS;
            default:
                throw new UnsupportedOperationException(format("unrecognized window frame type %s", type));
        }
    }

    public static BoundType toBoundType(FrameBound.Type type)
    {
        switch (type) {
            case UNBOUNDED_PRECEDING:
                return UNBOUNDED_PRECEDING;
            case PRECEDING:
                return PRECEDING;
            case CURRENT_ROW:
                return CURRENT_ROW;
            case FOLLOWING:
                return FOLLOWING;
            case UNBOUNDED_FOLLOWING:
                return UNBOUNDED_FOLLOWING;
            default:
                throw new UnsupportedOperationException(format("unrecognized frame bound type %s", type));
        }
    }
}
