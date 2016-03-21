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
package com.facebook.presto.execution;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Parameter;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class ParameterRewriter
        extends ExpressionRewriter<Void>
{
    private final List<Literal> parameterValues;

    public ParameterRewriter(List<Literal> parameterValues)
    {
        requireNonNull(parameterValues, "parameterValues is null");
        this.parameterValues = parameterValues;
    }

    @Override
    public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        if (parameterValues.size() <= node.getPosition()) {
            throw new PrestoException(INTERNAL_ERROR, "Too few parameters specified for query");
        }
        return parameterValues.get(node.getPosition());
    }
}
