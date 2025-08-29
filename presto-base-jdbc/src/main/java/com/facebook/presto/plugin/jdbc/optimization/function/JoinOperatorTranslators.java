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

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.forwardBindVariables;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.infixOperation;

/***
 * This class is a copy of OperatorTranslators class.
 * This class translate scalar operations to sql expression.
 */
//TODO : Once https://github.com/prestodb/presto/pull/23642 is merged this class can be removed and we can use the new annotation mechanism
public class JoinOperatorTranslators
{
    private JoinOperatorTranslators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static JdbcExpression add(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("+", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static JdbcExpression subtract(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("-", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equal(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalSmallInt(@SqlType(StandardTypes.SMALLINT) JdbcExpression left, @SqlType(StandardTypes.SMALLINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalTinyInteger(@SqlType(StandardTypes.TINYINT) JdbcExpression left, @SqlType(StandardTypes.TINYINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalDecimal(@SqlType(StandardTypes.DECIMAL) JdbcExpression left, @SqlType(StandardTypes.DECIMAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalReal(@SqlType(StandardTypes.REAL) JdbcExpression left, @SqlType(StandardTypes.REAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalDouble(@SqlType(StandardTypes.DOUBLE) JdbcExpression left, @SqlType(StandardTypes.DOUBLE) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalCharacter(@SqlType(StandardTypes.CHAR) JdbcExpression left, @SqlType(StandardTypes.CHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalVarchar(@SqlType(StandardTypes.VARCHAR) JdbcExpression left, @SqlType(StandardTypes.VARCHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalInteger(@SqlType(StandardTypes.INTEGER) JdbcExpression left, @SqlType(StandardTypes.INTEGER) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqual(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualInteger(@SqlType(StandardTypes.INTEGER) JdbcExpression left, @SqlType(StandardTypes.INTEGER) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualSmallInt(@SqlType(StandardTypes.SMALLINT) JdbcExpression left, @SqlType(StandardTypes.SMALLINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualTinyInt(@SqlType(StandardTypes.TINYINT) JdbcExpression left, @SqlType(StandardTypes.TINYINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualDecimal(@SqlType(StandardTypes.DECIMAL) JdbcExpression left, @SqlType(StandardTypes.DECIMAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualReal(@SqlType(StandardTypes.REAL) JdbcExpression left, @SqlType(StandardTypes.REAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualDouble(@SqlType(StandardTypes.DOUBLE) JdbcExpression left, @SqlType(StandardTypes.DOUBLE) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualVarchar(@SqlType(StandardTypes.VARCHAR) JdbcExpression left, @SqlType(StandardTypes.VARCHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualChar(@SqlType(StandardTypes.CHAR) JdbcExpression left, @SqlType(StandardTypes.CHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarFunction("not")
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression not(@SqlType(StandardTypes.BOOLEAN) JdbcExpression expression)
    {
        return new JdbcExpression(String.format("(NOT(%s))", expression.getExpression()), expression.getBoundConstantValues());
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThan(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanInteger(@SqlType(StandardTypes.INTEGER) JdbcExpression left, @SqlType(StandardTypes.INTEGER) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanTinyInt(@SqlType(StandardTypes.TINYINT) JdbcExpression left, @SqlType(StandardTypes.TINYINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanSmallInt(@SqlType(StandardTypes.SMALLINT) JdbcExpression left, @SqlType(StandardTypes.SMALLINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanReal(@SqlType(StandardTypes.REAL) JdbcExpression left, @SqlType(StandardTypes.REAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanDouble(@SqlType(StandardTypes.DOUBLE) JdbcExpression left, @SqlType(StandardTypes.DOUBLE) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanDecimal(@SqlType(StandardTypes.DECIMAL) JdbcExpression left, @SqlType(StandardTypes.DECIMAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanVarchar(@SqlType(StandardTypes.VARCHAR) JdbcExpression left, @SqlType(StandardTypes.VARCHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanChar(@SqlType(StandardTypes.CHAR) JdbcExpression left, @SqlType(StandardTypes.CHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThan(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanInteger(@SqlType(StandardTypes.INTEGER) JdbcExpression left, @SqlType(StandardTypes.INTEGER) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanTinyInt(@SqlType(StandardTypes.TINYINT) JdbcExpression left, @SqlType(StandardTypes.TINYINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanSmallInt(@SqlType(StandardTypes.SMALLINT) JdbcExpression left, @SqlType(StandardTypes.SMALLINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanReal(@SqlType(StandardTypes.REAL) JdbcExpression left, @SqlType(StandardTypes.REAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanDouble(@SqlType(StandardTypes.DOUBLE) JdbcExpression left, @SqlType(StandardTypes.DOUBLE) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanDecimal(@SqlType(StandardTypes.DECIMAL) JdbcExpression left, @SqlType(StandardTypes.DECIMAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanVarchar(@SqlType(StandardTypes.VARCHAR) JdbcExpression left, @SqlType(StandardTypes.VARCHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanChar(@SqlType(StandardTypes.CHAR) JdbcExpression left, @SqlType(StandardTypes.CHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqual(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualInteger(@SqlType(StandardTypes.INTEGER) JdbcExpression left, @SqlType(StandardTypes.INTEGER) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualTinyInt(@SqlType(StandardTypes.TINYINT) JdbcExpression left, @SqlType(StandardTypes.TINYINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualSmallInt(@SqlType(StandardTypes.SMALLINT) JdbcExpression left, @SqlType(StandardTypes.SMALLINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualReal(@SqlType(StandardTypes.REAL) JdbcExpression left, @SqlType(StandardTypes.REAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualDouble(@SqlType(StandardTypes.DOUBLE) JdbcExpression left, @SqlType(StandardTypes.DOUBLE) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualDecimal(@SqlType(StandardTypes.DECIMAL) JdbcExpression left, @SqlType(StandardTypes.DECIMAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualVarchar(@SqlType(StandardTypes.VARCHAR) JdbcExpression left, @SqlType(StandardTypes.VARCHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression greaterThanOrEqualChar(@SqlType(StandardTypes.CHAR) JdbcExpression left, @SqlType(StandardTypes.CHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation(">=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqual(@SqlType(StandardTypes.BIGINT) JdbcExpression left, @SqlType(StandardTypes.BIGINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualInteger(@SqlType(StandardTypes.INTEGER) JdbcExpression left, @SqlType(StandardTypes.INTEGER) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualTinyInt(@SqlType(StandardTypes.TINYINT) JdbcExpression left, @SqlType(StandardTypes.TINYINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualSmallInt(@SqlType(StandardTypes.SMALLINT) JdbcExpression left, @SqlType(StandardTypes.SMALLINT) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualReal(@SqlType(StandardTypes.REAL) JdbcExpression left, @SqlType(StandardTypes.REAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualDouble(@SqlType(StandardTypes.DOUBLE) JdbcExpression left, @SqlType(StandardTypes.DOUBLE) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualDecimal(@SqlType(StandardTypes.DECIMAL) JdbcExpression left, @SqlType(StandardTypes.DECIMAL) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualVarchar(@SqlType(StandardTypes.VARCHAR) JdbcExpression left, @SqlType(StandardTypes.VARCHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression lessThanOrEqualChar(@SqlType(StandardTypes.CHAR) JdbcExpression left, @SqlType(StandardTypes.CHAR) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<=", left, right), forwardBindVariables(left, right));
    }

    /*
     * Boolean datatype operations are specific to database.
     * For JdbcJoinPushdwon we are considering only common operation that are same to all database.
     * Here for boolean datatype we are not translating <, >, <= and >= as it is not working for some jdbc sources like Informix.
     */

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression equalBoolean(@SqlType(StandardTypes.BOOLEAN) JdbcExpression left, @SqlType(StandardTypes.BOOLEAN) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcExpression notEqualBoolean(@SqlType(StandardTypes.BOOLEAN) JdbcExpression left, @SqlType(StandardTypes.BOOLEAN) JdbcExpression right)
    {
        return new JdbcExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }
}
