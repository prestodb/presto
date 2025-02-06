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
package com.facebook.presto.rewriter;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Locale;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestOptPlusPassThroughMechanism
        extends AbstractOptPlusTestFramework
{
    public static String extractQueryName(String sql)
    {
        checkArgument(sql.startsWith("--q"));
        // i.e. Each sql query starts with -- 3 character long query name e.g. --q01
        return sql.substring(2, 5);
    }

    public static String getQueryResultPath(String queryName)
    {
        return format("/pass_through/%s.result", queryName);
    }

    @Test(timeOut = 240_000, dataProvider = "getQueriesDataProvider")
    public void testGenerateTestData(String queryResourcePath)
            throws IOException
    {
        if (getTestGenerateResultFiles()) {
            String sql = read(queryResourcePath)
                    .replaceAll("<schema>", getDb2TpchSchema().toUpperCase(Locale.getDefault()))
                    .replaceAll("<catalog>", getDb2TpchCatalog());
            String resultAsCSV = csvFromMaterializedResult(getQueryRunner().execute(getSession(), sql));
            resultAsCSV = resultAsCSV.replaceAll(getDb2TpchCatalog(), "<catalog>");
            resultAsCSV = resultAsCSV.replaceAll(getDb2TpchSchema(), "<schema>");
            generateResultsFiles(resultAsCSV,
                    getQueryResultPath(extractQueryName(sql)));
        }
    }

    protected void testQuery(String queryResourcePath)
    {
        // Presto asserts that schema names be small case, this is not the case with db2.
        @Language("SQL") String sql = read(queryResourcePath)
                .replaceAll("<schema>", getDb2TpchSchema().toUpperCase(Locale.getDefault()))
                .replaceAll("<catalog>", getDb2TpchCatalog());
        assertQueryInternal(sql);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
    {
        return new StoredResultExpectedQueryRunner(TestOptPlusPassThroughMechanism::getQueryResultPath,
                TestOptPlusPassThroughMechanism::extractQueryName);
    }

    @Override
    protected boolean enableFallback()
    {
        return false;
    }

    @Override
    protected Stream<String> getQueryPaths()
    {
        return ImmutableList.of("/pass_through/q01.sql").stream();
    }
}
