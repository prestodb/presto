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
package com.facebook.presto.spi.relation.column;

import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class CallExpression
        extends ColumnExpression
{
    private final Signature signature;
    private final Type returnType;
    private final List<ColumnExpression> arguments;

    public CallExpression(Signature signature, Type returnType, List<ColumnExpression> arguments)
    {
        requireNonNull(signature, "signature is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(returnType, "returnType is null");

        this.signature = signature;
        this.returnType = returnType;
        this.arguments = unmodifiableList(new ArrayList(arguments));
    }

    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public Type getType()
    {
        return returnType;
    }

    public List<ColumnExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        StringBuilder callExpression = new StringBuilder(signature.getName());
        callExpression.append("(").append(arguments.get(0));
        for (int i = 1; i < arguments.size(); i++) {
            callExpression.append(",").append(arguments.get(i));
        }
        callExpression.append(")");
        return callExpression.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, arguments);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CallExpression other = (CallExpression) obj;
        return Objects.equals(this.signature, other.signature) && Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public ColumnExpression replaceChildren(List<ColumnExpression> newChildren)
    {
        return new CallExpression(signature, returnType, newChildren);
    }

    @Override
    public <R, C> R accept(ColumnExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
