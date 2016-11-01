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
package com.facebook.presto.operator.window;

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.google.common.base.Preconditions.checkArgument;

@Test
public abstract class AbstractTestWindowFunction
{
    protected final LocalQueryRunner queryRunner;

    protected AbstractTestWindowFunction()
    {
        queryRunner = new LocalQueryRunner(TEST_SESSION);
    }

    @AfterClass
    public void tearDown()
    {
        queryRunner.close();
    }

    protected void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQuery(sql, expected, queryRunner);
    }

    protected void assertUnboundedWindowQuery(@Language("SQL") String sql, MaterializedResult expected)
    {
        assertWindowQuery(unbounded(sql), expected);
    }

    protected void assertWindowQueryWithNulls(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQueryWithNulls(sql, expected, queryRunner);
    }

    protected MaterializedResult executeWindowQueryWithNulls(@Language("SQL") String sql)
    {
        return WindowAssertions.executeWindowQueryWithNulls(sql, queryRunner);
    }

    protected void assertUnboundedWindowQueryWithNulls(@Language("SQL") String sql, MaterializedResult expected)
    {
        assertWindowQueryWithNulls(unbounded(sql), expected);
    }

    @Language("SQL")
    private static String unbounded(@Language("SQL") String sql)
    {
        checkArgument(sql.endsWith(")"), "SQL does not end with ')'");
        return sql.substring(0, sql.length() - 1) + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
    }
}
