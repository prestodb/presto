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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SpecialFormExpression
        extends RowExpression
{
    private final Form form;
    private final Type returnType;
    private final List<RowExpression> arguments;

    public SpecialFormExpression(Form form, Type returnType, RowExpression... arguments)
    {
        this(form, returnType, ImmutableList.copyOf(arguments));
    }

    public SpecialFormExpression(Form form, Type returnType, List<RowExpression> arguments)
    {
        this.form = requireNonNull(form, "form is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
    }

    public Form getForm()
    {
        return form;
    }

    @Override
    public Type getType()
    {
        return returnType;
    }

    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return form.name() + "(" + Joiner.on(", ").join(arguments) + ")";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(form, arguments);
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
        SpecialFormExpression other = (SpecialFormExpression) obj;
        return this.form == other.form &&
                Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitSpecialForm(this, context);
    }

    public enum Form
    {
        IF,
        NULL_IF,
        SWITCH,
        WHEN,
        IS_NULL,
        COALESCE,
        IN,
        AND,
        OR,
        DEREFERENCE,
        ROW_CONSTRUCTOR,
        BIND,
    }
}
