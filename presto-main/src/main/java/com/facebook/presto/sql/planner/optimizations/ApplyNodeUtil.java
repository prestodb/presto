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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.InSubqueryExpression;
import com.facebook.presto.spi.relation.QuantifiedComparisonExpression;
import com.facebook.presto.spi.relation.RowExpression;

import static com.google.common.base.Preconditions.checkArgument;

public final class ApplyNodeUtil
{
    private ApplyNodeUtil() {}

    public static void verifySubquerySupported(Assignments assignments)
    {
        checkArgument(
                assignments.getExpressions().stream().allMatch(ApplyNodeUtil::isSupportedSubqueryExpression),
                "Unexpected expression used for subquery expression: %s", assignments.getExpressions());
    }

    private static boolean isSupportedSubqueryExpression(RowExpression expression)
    {
        return expression instanceof InSubqueryExpression ||
                expression instanceof QuantifiedComparisonExpression ||
                expression instanceof ExistsExpression;
    }
}
