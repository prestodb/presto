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
package com.facebook.presto.sql.routine;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SqlVariable
        implements SqlNode
{
    private final int field;
    private final Type type;
    private final RowExpression defaultValue;

    public SqlVariable(int field, Type type, RowExpression defaultValue)
    {
        this.field = field;
        this.type = requireNonNull(type, "type is null");
        this.defaultValue = requireNonNull(defaultValue, "value is null");
    }

    public int getField()
    {
        return field;
    }

    public Type getType()
    {
        return type;
    }

    public RowExpression getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitVariable(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("field", field)
                .add("type", type)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
