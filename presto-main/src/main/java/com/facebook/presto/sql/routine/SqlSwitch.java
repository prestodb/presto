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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlSwitch
        implements SqlStatement
{
    private final RowExpression value;
    private final List<WhenClause> cases;
    private final SqlBlock defaultBlock;

    public SqlSwitch(RowExpression value, List<WhenClause> cases, SqlBlock defaultBlock)
    {
        this.value = requireNonNull(value, "value is null");
        this.cases = requireNonNull(cases, "cases is null");
        this.defaultBlock = requireNonNull(defaultBlock, "defaultBlock is null");
    }

    public RowExpression getValue()
    {
        return value;
    }

    public List<WhenClause> getCases()
    {
        return cases;
    }

    public SqlBlock getDefaultBlock()
    {
        return defaultBlock;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitSwitch(this, context);
    }
}
