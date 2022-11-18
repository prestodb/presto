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
package com.facebook.presto.sql.analyzer.crux;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CallExpression
        extends Expression
{
    private final List<Expression> arguments;
    private final FunctionExpression function;
    private final boolean isDistinct;

    public CallExpression(CodeLocation location, Type type, boolean isDistinct, FunctionExpression function, List<Expression> arguments)
    {
        super(ExpressionKind.CALL, location, type);
        this.isDistinct = isDistinct;
        this.function = requireNonNull(function, "function is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public FunctionExpression getFunction()
    {
        return function;
    }

    public String getName()
    {
        return function.getFunction().getName();
    }

    public boolean getIsDistinct()
    {
        return isDistinct;
    }

    /**
     * @return an immutable list of the arguments to the function
     */
    List<Expression> getArguments()
    {
        return arguments;
    }
}
