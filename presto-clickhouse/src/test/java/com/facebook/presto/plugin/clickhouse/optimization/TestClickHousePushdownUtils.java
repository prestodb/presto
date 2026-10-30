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
package com.facebook.presto.plugin.clickhouse.optimization;

import com.facebook.presto.spi.relation.ConstantExpression;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.clickhouse.optimization.ClickHousePushdownUtils.getLiteralAsString;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestClickHousePushdownUtils
{
    @Test
    public void testVarcharLiteralIsEscaped()
    {
        assertEquals(varcharLiteral("Campbell"), "'Campbell'");
        assertEquals(varcharLiteral("O'Brien"), "'O\\'Brien'");
        assertEquals(varcharLiteral("x' OR 1 = 1 --"), "'x\\' OR 1 = 1 --'");
        assertEquals(varcharLiteral("x\\' OR 1 = 1 --"), "'x\\\\\\' OR 1 = 1 --'");
    }

    private static String varcharLiteral(String value)
    {
        return getLiteralAsString(SESSION, new ConstantExpression(utf8Slice(value), VARCHAR));
    }
}
