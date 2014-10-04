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
package com.facebook.presto.sql;

import com.facebook.presto.SessionTestUtils;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import org.testng.annotations.Test;

import java.util.IdentityHashMap;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestSqlToRowExpressionTranslator
{
    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new LongLiteral("1");
        IdentityHashMap<Expression, Type> types = new IdentityHashMap<>();
        types.put(expression, BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CoalesceExpression(expression);
            types.put(expression, BIGINT);
        }
        SqlToRowExpressionTranslator.translate(expression, types, new MetadataManager(), SessionTestUtils.TEST_SESSION, true);
    }
}
