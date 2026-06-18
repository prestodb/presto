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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.spi.relation.ConstantExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class JdbcExpression
{
    private final String expression;
    private final List<ConstantExpression> boundConstantValues;

    public JdbcExpression(String expression)
    {
        this(expression, ImmutableList.of());
    }

    @JsonCreator
    public JdbcExpression(
            @JsonProperty("translatedString") String expression,
            @JsonProperty("boundConstantValues") List<ConstantExpression> constantBindValues)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.boundConstantValues = requireNonNull(constantBindValues, "boundConstantValues is null");
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    /**
     * Constant expressions are not added to the expression String. Instead they appear as "?" in the query.
     * This is because we would potentially lose precision on double values. Hence when we make a PreparedStatement
     * out of the SQL string replacing every "?" by it's corresponding actual bindValue.
     *
     * @return List of constants to replace in the SQL string.
     */
    @JsonProperty
    public List<ConstantExpression> getBoundConstantValues()
    {
        return boundConstantValues;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcExpression that = (JdbcExpression) o;
        return expression.equals(that.expression) &&
                boundConstantValues.equals(that.boundConstantValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, boundConstantValues);
    }
}
