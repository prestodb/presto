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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType;
import com.facebook.presto.sql.tree.Expression;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowFrameProvider
        implements ExpectedValueProvider<WindowNode.Frame>
{
    private final WindowType type;
    private final BoundType startType;
    private final Optional<SymbolAlias> startValue;
    private final Optional<SymbolAlias> sortKeyForStartComparison;
    private final BoundType endType;
    private final Optional<SymbolAlias> endValue;
    private final Optional<SymbolAlias> sortKeyForEndComparison;

    private Optional<Type> startValueType;
    private Optional<Type> sortKeyForStartComparisonType;
    private Optional<Type> endValueType;
    private Optional<Type> sortKeyForEndComparisonType;

    WindowFrameProvider(
            WindowType type,
            BoundType startType,
            Optional<SymbolAlias> startValue,
            Optional<Type> startValueType,
            Optional<SymbolAlias> sortKeyForStartComparison,
            Optional<Type> sortKeyForStartComparisonType,
            BoundType endType,
            Optional<SymbolAlias> endValue,
            Optional<Type> endValueType,
            Optional<SymbolAlias> sortKeyForEndComparison,
            Optional<Type> sortKeyForEndComparisonType)
    {
        this.type = requireNonNull(type, "type is null");
        this.startType = requireNonNull(startType, "startType is null");
        this.startValue = requireNonNull(startValue, "startValue is null");
        this.startValueType = requireNonNull(startValueType, "startValueType is null");
        this.sortKeyForStartComparison = requireNonNull(sortKeyForStartComparison, "sortKeyForStartComparison is null");
        this.sortKeyForStartComparisonType = requireNonNull(sortKeyForStartComparisonType, "sortKeyForStartComparisonType is null");
        this.endType = requireNonNull(endType, "endType is null");
        this.endValue = requireNonNull(endValue, "endValue is null");
        this.endValueType = requireNonNull(endValueType, "endValueType is null");
        this.sortKeyForEndComparison = requireNonNull(sortKeyForEndComparison, "sortKeyForEndComparison is null");
        this.sortKeyForEndComparisonType = requireNonNull(sortKeyForEndComparisonType, "sortKeyForEndComparisonType is null");
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
                toVariableReferenceExpression(aliases, startValue, startValueType),
                toVariableReferenceExpression(aliases, sortKeyForStartComparison, sortKeyForStartComparisonType),
                endType,
                toVariableReferenceExpression(aliases, endValue, endValueType),
                toVariableReferenceExpression(aliases, sortKeyForEndComparison, sortKeyForEndComparisonType),
                originalStartValue.map(Expression::toString),
                originalEndValue.map(Expression::toString));
    }

    private Optional<VariableReferenceExpression> toVariableReferenceExpression(SymbolAliases aliases, Optional<SymbolAlias> symbolAlias, Optional<Type> type)
    {
        if (!symbolAlias.isPresent()) {
            return Optional.empty();
        }

        String alias = symbolAlias.get().toSymbol(aliases).getName();
        Type variableType = type.orElseGet(() -> BIGINT);
        // This is really ugly! Because we translate differently between field and other expressions in TranslationMap.get().
        // We should handle them the same way here but we don't have information other than their names. Luckily this is test code.
        if (alias.startsWith("field")) {
            return Optional.of(new VariableReferenceExpression(Optional.empty(), symbolAlias.get().toString(), variableType));
        }

        return Optional.of(new VariableReferenceExpression(Optional.empty(), symbolAlias.get().toSymbol(aliases).getName(), variableType));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", this.type)
                .add("startType", this.startType)
                .add("startValue", this.startValue)
                .add("startValueType", this.startValueType)
                .add("sortKeyForStartComparison", this.sortKeyForStartComparison)
                .add("sortKeyForStartComparisonType", this.sortKeyForStartComparisonType)
                .add("endType", this.endType)
                .add("endValue", this.endValue)
                .add("endValueType", this.endValueType)
                .add("sortKeyForEndComparison", this.sortKeyForEndComparison)
                .add("sortKeyForEndComparisonType", this.sortKeyForEndComparisonType)
                .toString();
    }
}
