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
package com.facebook.presto.plugin.jdbc.optimization.function;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlSignature;
import com.facebook.presto.spi.function.SupportedSignatures;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.forwardBindVariables;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.infixOperation;

public class OperatorTranslators
{
    private OperatorTranslators()
    {
    }

    @ScalarOperator(ADD)
    @SupportedSignatures({
            @SqlSignature(argumentType = StandardTypes.BIGINT, returnType = StandardTypes.BIGINT),
            @SqlSignature(argumentType = StandardTypes.INTEGER, returnType = StandardTypes.INTEGER)})
    public static JdbcExpression add(JdbcExpression left, JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("+", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(SUBTRACT)
    @SupportedSignatures(@SqlSignature(argumentType = StandardTypes.BIGINT, returnType = StandardTypes.BIGINT))
    public static JdbcExpression subtract(JdbcExpression left, JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("-", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SupportedSignatures({
            @SqlSignature(argumentType = StandardTypes.BIGINT, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.INTEGER, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.SMALLINT, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.TINYINT, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.BOOLEAN, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.DATE, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.DECIMAL, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.REAL, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.DOUBLE, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.VARCHAR, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.TIMESTAMP, returnType = StandardTypes.BOOLEAN)})
    public static JdbcExpression equal(JdbcExpression left, JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SupportedSignatures({
            @SqlSignature(argumentType = StandardTypes.BIGINT, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.INTEGER, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.SMALLINT, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.TINYINT, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.BOOLEAN, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.DATE, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.DECIMAL, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.REAL, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.DOUBLE, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.VARCHAR, returnType = StandardTypes.BOOLEAN),
            @SqlSignature(argumentType = StandardTypes.TIMESTAMP, returnType = StandardTypes.BOOLEAN)})
    public static JdbcExpression notEqual(JdbcExpression left, JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarFunction("not")
    @SupportedSignatures({@SqlSignature(argumentType = StandardTypes.BOOLEAN, returnType = StandardTypes.BOOLEAN)})
    public static JdbcExpression not(JdbcExpression expression)
    {
        return new JdbcExpression(String.format("(NOT(%s))", expression.getExpression()), expression.getBoundConstantValues());
    }
}
