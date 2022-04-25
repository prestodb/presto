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
package com.facebook.presto.plugin.clickhouse.optimization.function;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseExpression;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.plugin.clickhouse.optimization.function.ClickHouseTranslationUtil.forwardBindVariables;
import static com.facebook.presto.plugin.clickhouse.optimization.function.ClickHouseTranslationUtil.infixOperation;

public class OperatorTranslators
{
    private OperatorTranslators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static ClickHouseExpression add(@SqlType(StandardTypes.BIGINT) ClickHouseExpression left, @SqlType(StandardTypes.BIGINT) ClickHouseExpression right)
    {
        return new ClickHouseExpression(infixOperation("+", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static ClickHouseExpression subtract(@SqlType(StandardTypes.BIGINT) ClickHouseExpression left, @SqlType(StandardTypes.BIGINT) ClickHouseExpression right)
    {
        return new ClickHouseExpression(infixOperation("-", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static ClickHouseExpression equal(@SqlType(StandardTypes.BIGINT) ClickHouseExpression left, @SqlType(StandardTypes.BIGINT) ClickHouseExpression right)
    {
        return new ClickHouseExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static ClickHouseExpression notEqual(@SqlType(StandardTypes.BIGINT) ClickHouseExpression left, @SqlType(StandardTypes.BIGINT) ClickHouseExpression right)
    {
        return new ClickHouseExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarFunction("not")
    @SqlType(StandardTypes.BOOLEAN)
    public static ClickHouseExpression not(@SqlType(StandardTypes.BOOLEAN) ClickHouseExpression expression)
    {
        return new ClickHouseExpression(String.format("(NOT(%s))", expression.getExpression()), expression.getBoundConstantValues());
    }
}
