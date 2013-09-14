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

import com.facebook.presto.util.MaterializedResult;
import org.intellij.lang.annotations.Language;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.AbstractTestQueries.assertEqualsIgnoreOrder;
import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;
import static java.lang.String.format;

public final class WindowAssertions
{
    private WindowAssertions() {}

    public static MaterializedResult computeActual(@Language("SQL") String sql, ExecutorService executor)
    {
        return createTpchLocalQueryRunner(executor).execute(sql);
    }

    public static void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected, ExecutorService executor)
    {
        @Language("SQL") String query = format("" +
                "SELECT orderkey, orderstatus,\n%s\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "ORDER BY orderkey", sql);

        MaterializedResult actual = computeActual(query, executor);
        assertEqualsIgnoreOrder(actual.getMaterializedTuples(), expected.getMaterializedTuples());
    }
}
