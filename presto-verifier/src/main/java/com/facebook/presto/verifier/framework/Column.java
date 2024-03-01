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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.verifier.framework.Column.Category.ARRAY;
import static com.facebook.presto.verifier.framework.Column.Category.FLOATING_POINT;
import static com.facebook.presto.verifier.framework.Column.Category.MAP;
import static com.facebook.presto.verifier.framework.Column.Category.ROW;
import static com.facebook.presto.verifier.framework.Column.Category.SIMPLE;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static java.util.Objects.requireNonNull;

public class Column
{
    public enum Category
    {
        SIMPLE,
        FLOATING_POINT,
        ARRAY,
        ROW,
        MAP,
    }

    private static final Set<Type> FLOATING_POINT_TYPES = ImmutableSet.of(DOUBLE, REAL);

    private final String name;
    private final Expression expression;
    private final Category category;
    private final Type type;

    private Column(String name, Expression expression, Category category, Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.category = requireNonNull(category, "kind is null");
        this.type = requireNonNull(type, "type is null");
    }

    public String getName()
    {
        return name;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Category getCategory()
    {
        return category;
    }

    public Type getType()
    {
        return type;
    }

    public static Column fromResultSet(TypeManager typeManager, ResultSet resultSet)
            throws SQLException
    {
        String columnName = resultSet.getString("Column");
        Type type = typeManager.getType(parseTypeSignature(resultSet.getString("Type")));
        return create(columnName, delimitedIdentifier(columnName), type);
    }

    public static Column create(String name, Expression expression, Type type)
    {
        Category category;
        if (FLOATING_POINT_TYPES.contains(type)) {
            category = FLOATING_POINT;
        }
        else if (type instanceof ArrayType) {
            category = ARRAY;
        }
        else if (type instanceof MapType) {
            category = MAP;
        }
        else if (type instanceof RowType) {
            category = ROW;
        }
        else {
            category = SIMPLE;
        }
        return new Column(name, expression, category, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Column o = (Column) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(category, o.category) &&
                Objects.equals(type, o.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, category, type);
    }
}
