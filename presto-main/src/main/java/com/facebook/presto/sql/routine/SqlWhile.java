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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SqlWhile
        implements SqlStatement
{
    private final Optional<SqlLabel> label;
    private final RowExpression condition;
    private final SqlBlock body;

    public SqlWhile(RowExpression condition, SqlBlock body)
    {
        this(Optional.empty(), condition, body);
    }

    public SqlWhile(Optional<SqlLabel> label, RowExpression condition, SqlBlock body)
    {
        this.label = requireNonNull(label, "label is null");
        this.condition = requireNonNull(condition, "condition is null");
        this.body = requireNonNull(body, "body is null");
    }

    public Optional<SqlLabel> getLabel()
    {
        return label;
    }

    public RowExpression getCondition()
    {
        return condition;
    }

    public SqlBlock getBody()
    {
        return body;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitWhile(this, context);
    }
}
