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

import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.relation.ConstantExpression;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class JdbcTranslationUtil
{
    private JdbcTranslationUtil()
    {
    }

    public static String infixOperation(String operator, JdbcExpression left, JdbcExpression right)
    {
        return String.format("(%s %s %s)", left.getExpression(), operator, right.getExpression());
    }

    public static List<ConstantExpression> forwardBindVariables(JdbcExpression... jdbcExpressions)
    {
        return Arrays.stream(jdbcExpressions).map(JdbcExpression::getBoundConstantValues)
                .flatMap(List::stream)
                .collect(toImmutableList());
    }
}
