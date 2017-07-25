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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DereferenceExpression
        extends Expression
{
    private final Expression base;
    private final Identifier field;

    public DereferenceExpression(Expression base, Identifier field)
    {
        this(Optional.empty(), base, field);
    }

    public DereferenceExpression(NodeLocation location, Expression base, Identifier field)
    {
        this(Optional.of(location), base, field);
    }

    private DereferenceExpression(Optional<NodeLocation> location, Expression base, Identifier field)
    {
        super(location);
        checkArgument(base != null, "base is null");
        checkArgument(field != null, "fieldName is null");
        this.base = base;
        this.field = field;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDereferenceExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(base);
    }

    public Expression getBase()
    {
        return base;
    }

    public Identifier getField()
    {
        return field;
    }

    /**
     * If this DereferenceExpression looks like a QualifiedName, return QualifiedName.
     * Otherwise return null
     */
    public static QualifiedName getQualifiedName(DereferenceExpression expression)
    {
        List<String> parts = tryParseParts(expression.base, expression.field.getValue().toLowerCase(Locale.ENGLISH));
        return parts == null ? null : QualifiedName.of(parts);
    }

    public static Expression from(QualifiedName name)
    {
        Expression result = null;

        for (String part : name.getParts()) {
            if (result == null) {
                result = new Identifier(part);
            }
            else {
                result = new DereferenceExpression(result, new Identifier(part));
            }
        }

        return result;
    }

    private static List<String> tryParseParts(Expression base, String fieldName)
    {
        if (base instanceof Identifier) {
            return ImmutableList.of(((Identifier) base).getValue(), fieldName);
        }
        else if (base instanceof DereferenceExpression) {
            QualifiedName baseQualifiedName = getQualifiedName((DereferenceExpression) base);
            if (baseQualifiedName != null) {
                List<String> newList = new ArrayList<>(baseQualifiedName.getParts());
                newList.add(fieldName);
                return newList;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DereferenceExpression that = (DereferenceExpression) o;
        return Objects.equals(base, that.base) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, field);
    }
}
