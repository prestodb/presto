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

import com.facebook.presto.sql.relational.RowExpression;

import static java.util.Objects.requireNonNull;

public class SqlSet
        implements SqlStatement
{
    private final SqlVariable target;
    private final RowExpression value;

    public SqlSet(SqlVariable target, RowExpression value)
    {
        this.target = requireNonNull(target, "target is null");
        this.value = requireNonNull(value, "value is null");
    }

    public SqlVariable getTarget()
    {
        return target;
    }

    public RowExpression getValue()
    {
        return value;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitSet(this, context);
    }
}
