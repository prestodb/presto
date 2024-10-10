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
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestOrderByQueries;
import org.testng.annotations.Test;

public class TestOrderByQueries
        extends AbstractTestOrderByQueries
{
    private static final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true, storageFormat);
    }

    @Override
    protected void createTables()
    {
        try {
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(storageFormat);
            NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Test
    public void testOrderByWithOutputColumnReference()
    {
        assertQueryOrdered("SELECT a*2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY b*-1", "VALUES 4, 0, -2");
        assertQueryOrdered("SELECT a*2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY b", "VALUES -2, 0, 4");
        assertQueryOrdered("SELECT a*-2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a*-1", "VALUES 2, 0, -4");
        assertQueryOrdered("SELECT a*-2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a*-2 FROM (VALUES -1, 0, 2) t(a) ORDER BY a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a*-2 FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a", "VALUES (-1, 1), (0, 0), (2, -2)");
        assertQueryOrdered("SELECT a, a* -2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY a + b", "VALUES (2, -4), (0, 0), (-1, 2)");
        assertQueryOrdered("SELECT a AS b, a* -2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a + b", "VALUES (2, -4), (0, 0), (-1, 2)");
        assertQueryOrdered("SELECT a* -2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a + t.a", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT k, SUM(a) a, SUM(b) a FROM (VALUES (1, 2, 3)) t(k, a, b) GROUP BY k ORDER BY k", "VALUES (1, 2, 3)");

        // coercions
        assertQueryOrdered("SELECT 1 x ORDER BY degrees(x)", "VALUES 1");
        assertQueryOrdered("SELECT a + 1 AS b FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * b", "VALUES 3, 2");
        assertQueryOrdered("SELECT a AS b FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * b", "VALUES 2, 1");
        assertQueryOrdered("SELECT a AS a FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * a", "VALUES 2, 1");
        assertQueryOrdered("SELECT 1 x ORDER BY degrees(x)", "VALUES 1");

        // groups
        assertQueryOrdered("SELECT max(a+b), min(a+b) AS a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(t.a+t.b)", "VALUES (5, 5), (6, 3)");
        assertQueryOrdered("SELECT max(a+b), min(a+b) AS a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(t.a+t.b)*-0.1", "VALUES (6, 3), (5, 5)");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1.0)", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a) AS b FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 1, 2");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b*1.0", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a)*100 AS c FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b) + c", "VALUES 100, 200");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY t.b ORDER BY t.b*1.0", "VALUES 2, 1");
        assertQueryOrdered("SELECT -(a+b) AS a, -(a+b) AS b, a+b FROM (values (41, 42), (-41, -42)) t(a,b) GROUP BY a+b ORDER BY a+b", "VALUES (-83, -83, 83), (83, 83, -83)");
        assertQueryOrdered("SELECT -a AS a FROM (values (1,2),(3,2)) t(a,b) GROUP BY GROUPING SETS ((a), (a, b)) ORDER BY -a", "VALUES -1, -1, -3, -3");
        assertQueryOrdered("SELECT a AS foo FROM (values (1,2),(3,2)) t(a,b) GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL ORDER BY -a", "VALUES 3, 1");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2),(3,2)) t(a,b) ORDER BY max(-a)", "VALUES 3");
        assertQueryFails("SELECT max(a) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY max(a+b)", ".*Invalid reference to output projection attribute from ORDER BY aggregation");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY t.a ORDER BY a", "VALUES (-2, 2), (-1, 1)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY t.a ORDER BY t.a", "VALUES (-1, 1), (-2, 2)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY a ORDER BY t.a", "VALUES (-1, 1), (-2, 2)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY a ORDER BY t.a+2*a", "VALUES (-2, 2), (-1, 1)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY t.a ORDER BY t.a+2*a", "VALUES (-2, 2), (-1, 1)");

        // lambdas
        assertQueryFails("SELECT x AS y FROM (values (1,2), (2,3)) t(x, y) GROUP BY x ORDER BY apply(x, x -> -x) + 2*x", ".*Scalar function name not registered: presto.default.apply.*");
        assertQueryFails("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY apply(x, x -> -x)", ".*Scalar function name not registered: presto.default.apply.*");
        assertQueryFails("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY sum(apply(-y, x -> x * 1.0))", ".*Scalar function name not registered: presto.default.apply.*");

        // distinct
        assertQueryOrdered("SELECT DISTINCT -a AS b FROM (VALUES 1, 2) t(a) ORDER BY b", "VALUES -2, -1");
        assertQueryOrdered("SELECT DISTINCT -a AS b FROM (VALUES 1, 2) t(a) ORDER BY 1", "VALUES -2, -1");
        assertQueryOrdered("SELECT DISTINCT max(a) AS b FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 1, 2");
        assertQueryFails("SELECT DISTINCT -a AS b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY c", ".*For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertQueryFails("SELECT DISTINCT -a AS b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY 2", ".*ORDER BY position 2 is not in select list");

        // window
        assertQueryOrdered("SELECT a FROM (VALUES 1, 2) t(a) ORDER BY -row_number() OVER ()", "VALUES 2, 1");
        assertQueryOrdered("SELECT -a AS a, first_value(-a) OVER (ORDER BY a ROWS 0 PRECEDING) AS b FROM (VALUES 1, 2) t(a) ORDER BY first_value(a) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES (-2, -2), (-1, -1)");
        assertQueryOrdered("SELECT -a AS a FROM (VALUES 1, 2) t(a) ORDER BY first_value(a+t.a*2) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES -1, -2");

        assertQueryFails("SELECT a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a", ".*'a' is ambiguous");
    }
}
