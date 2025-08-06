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
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeConstraint;
import com.facebook.presto.spi.function.TypeConstraints;
import com.facebook.presto.spi.function.TypeParameterBinding;

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
    @TypeConstraints({
            @TypeConstraint({
                    @TypeParameterBinding(parameter = "T", type = StandardTypes.INTEGER),
                    @TypeParameterBinding(parameter = "U", type = StandardTypes.INTEGER)
            }),
            @TypeConstraint({
                    @TypeParameterBinding(parameter = "T", type = StandardTypes.BIGINT),
                    @TypeParameterBinding(parameter = "U", type = StandardTypes.INTEGER)
            }),
            @TypeConstraint({
                    @TypeParameterBinding(parameter = "T", type = StandardTypes.BIGINT),
                    @TypeParameterBinding(parameter = "U", type = StandardTypes.BIGINT)
            })})
    @SqlType("T")
    public static JdbcExpression add(
            @SqlType("T") JdbcExpression left,
            @SqlType("U") JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("+", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static JdbcExpression subtract(
            @SqlType(StandardTypes.BIGINT) JdbcExpression left,
            @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("-", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @TypeConstraints({
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.BIGINT)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.INTEGER)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.SMALLINT)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.TINYINT)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.BOOLEAN)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.DATE)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.DECIMAL)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.REAL)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.DOUBLE)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.VARCHAR)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.TIMESTAMP)})})
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equal(
            @SqlType("T") JdbcExpression left,
            @SqlType("T") JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @TypeConstraints({
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.BIGINT)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.INTEGER)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.SMALLINT)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.TINYINT)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.BOOLEAN)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.DATE)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.DECIMAL)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.REAL)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.DOUBLE)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.VARCHAR)}),
            @TypeConstraint({@TypeParameterBinding(parameter = "T", type = StandardTypes.TIMESTAMP)})})
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqual(
            @SqlType("T") JdbcExpression left,
            @SqlType("T") JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarFunction("not")
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression not(@SqlType(StandardTypes.BOOLEAN) JdbcExpression expression)
    {
        return new JdbcExpression(String.format("(NOT(%s))", expression.getExpression()), expression.getBoundConstantValues());
    }
}
