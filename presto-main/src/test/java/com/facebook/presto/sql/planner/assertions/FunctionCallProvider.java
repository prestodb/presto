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

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Joiner;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.toSymbolReferences;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class FunctionCallProvider
    implements ExpectedValueProvider<FunctionCall>
{
    private boolean isWindowFunction;
    private final QualifiedName name;
    private final Optional<WindowFrame> frame;
    private final boolean distinct;
    private final List<PlanTestSymbol> args;

    private FunctionCallProvider(boolean isWindowFunction, QualifiedName name, Optional<WindowFrame> frame, boolean distinct, List<PlanTestSymbol> args)
    {
        this.isWindowFunction = isWindowFunction;
        this.name = requireNonNull(name, "name is null");
        this.frame = requireNonNull(frame, "frame is null");
        this.distinct = distinct;
        this.args = requireNonNull(args, "args is null");
    }

    FunctionCallProvider(QualifiedName name, Optional<WindowFrame> frame, boolean distinct, List<PlanTestSymbol> args)
    {
        this(true, name, frame, distinct, args);
    }

    FunctionCallProvider(QualifiedName name, boolean distinct, List<PlanTestSymbol> args)
    {
        this(false, name, Optional.empty(), distinct, args);
    }

    FunctionCallProvider(QualifiedName name, List<PlanTestSymbol> args)
    {
        this(false, name, Optional.empty(), false, args);
    }

    @Override
    public String toString()
    {
        return format("%s%s (%s) %s", distinct ? "DISTINCT" : "", name, Joiner.on(", ").join(args), frame.isPresent() ? frame.get().toString() : "");
    }

    public FunctionCall getExpectedValue(SymbolAliases aliases)
    {
        List<Expression> symbolReferences = toSymbolReferences(args, aliases);
        if (isWindowFunction) {
            return new ExpectedWindowFunctionCall(symbolReferences);
        }

        return new FunctionCall(name, symbolReferences);
    }

    private class ExpectedWindowFunctionCall
            extends FunctionCall
    {
        private ExpectedWindowFunctionCall(List<Expression> args)
        {
            super(name, distinct, args);
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }

            if (object == null || !(object instanceof FunctionCall)) {
                return false;
            }

            FunctionCall other = (FunctionCall) object;

            return Objects.equals(name, other.getName()) &&
                    other.getWindow().isPresent() &&
                    Objects.equals(frame, other.getWindow().get().getFrame()) &&
                    Objects.equals(distinct, other.isDistinct()) &&
                    Objects.equals(getArguments(), other.getArguments());
        }

        @Override
        public int hashCode()
        {
            /*
             * Putting this in a hash table is probably not a useful thing to do,
             * especially not if you want to compare this with an actual WindowFunction.
             * This is because (by necessity) ExpectedWindowFunctionCalls don't have the
             * same fields as FunctionCalls, and can't hash the same as a result.
             *
             * If you find a useful case for putting this in a hash table, feel free to
             * add an implementation. Until then, it would just be dead and untested code.
             */
            throw new UnsupportedOperationException("Test object");
        }
    }
}
