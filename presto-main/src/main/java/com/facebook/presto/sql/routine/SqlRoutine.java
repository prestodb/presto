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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlRoutine
        implements SqlNode
{
    private final Type returnType;
    private final List<SqlVariable> parameters;
    private final SqlStatement body;

    public SqlRoutine(Type returnType, List<SqlVariable> parameters, SqlStatement body)
    {
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.body = requireNonNull(body, "body is null");
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<SqlVariable> getParameters()
    {
        return parameters;
    }

    public SqlStatement getBody()
    {
        return body;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitRoutine(this, context);
    }
}
