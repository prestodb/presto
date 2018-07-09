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

import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowFrameProvider
        implements ExpectedValueProvider<WindowNode.Frame>
{
    private final WindowFrame.Type type;
    private final FrameBound.Type startType;
    private final Optional<SymbolAlias> startValue;
    private final FrameBound.Type endType;
    private final Optional<SymbolAlias> endValue;

    WindowFrameProvider(
            WindowFrame.Type type,
            FrameBound.Type startType,
            Optional<SymbolAlias> startValue,
            FrameBound.Type endType,
            Optional<SymbolAlias> endValue)
    {
        this.type = requireNonNull(type, "type is null");
        this.startType = requireNonNull(startType, "startType is null");
        this.startValue = requireNonNull(startValue, "startValue is null");
        this.endType = requireNonNull(endType, "endType is null");
        this.endValue = requireNonNull(endValue, "endValue is null");
    }

    @Override
    public WindowNode.Frame getExpectedValue(SymbolAliases aliases)
    {
        // synthetize original start/end value to keep the constructor of the frame happy. These are irrelevant for the purpose
        // of testing the plan structure.
        Optional<Expression> originalStartValue = startValue.map(alias -> alias.toSymbol(aliases).toSymbolReference());
        Optional<Expression> originalEndValue = endValue.map(alias -> alias.toSymbol(aliases).toSymbolReference());

        return new WindowNode.Frame(
                type,
                startType,
                startValue.map(alias -> alias.toSymbol(aliases)),
                endType,
                endValue.map(alias -> alias.toSymbol(aliases)),
                originalStartValue,
                originalEndValue);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", this.type)
                .add("startType", this.startType)
                .add("startValue", this.startValue)
                .add("endType", this.endType)
                .add("endValue", this.endValue)
                .toString();
    }
}
