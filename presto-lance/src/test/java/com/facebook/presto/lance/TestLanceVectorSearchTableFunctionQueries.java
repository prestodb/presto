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
package com.facebook.presto.lance;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLanceVectorSearchTableFunctionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Copy vector_db to a temp directory so the runner never writes to checked-in resources
        URL dbUrl = Resources.getResource(TestLanceVectorSearchTableFunctionQueries.class, "/vector_db");
        Path source = Paths.get(dbUrl.toURI());
        Path tempDir = Files.createTempDirectory("lance-tvf-test");
        copyDirectory(source, tempDir);
        return LanceQueryRunner.createLanceQueryRunner(
                ImmutableMap.of("lance.root", tempDir.toString()),
                ImmutableMap.of("http-server.http.port", "0"));
    }

    private static void copyDirectory(Path source, Path target)
            throws IOException
    {
        Files.walkFileTree(source, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException
            {
                Files.createDirectories(target.resolve(source.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                Files.copy(file, target.resolve(source.relativize(file)));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    public void testBasicTopK()
    {
        // row i has vec=[i,0,0,0]; L2 to [0.3,0,0,0] is (i-0.3)^2, strictly increasing in i
        assertQueryWithSameQueryRunner(
                "SELECT id FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.3, 0.0, 0.0, 0.0], k => 3)) ORDER BY _distance",
                "VALUES BIGINT '0', BIGINT '1', BIGINT '2'");
    }

    @Test
    public void testRowCountIsExactlyK()
    {
        assertQueryWithSameQueryRunner(
                "SELECT count(*) FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.3, 0.0, 0.0, 0.0], k => 5))",
                "SELECT BIGINT '5'");
    }

    @Test
    public void testDefaultK()
    {
        // Default k is 10
        assertQueryWithSameQueryRunner(
                "SELECT count(*) FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.3, 0.0, 0.0, 0.0]))",
                "SELECT BIGINT '10'");
    }

    @Test
    public void testDistanceValues()
    {
        // nearest row is id=0 at distance 0.3^2 = 0.09 (REAL)
        assertQueryWithSameQueryRunner(
                "SELECT id, round(CAST(_distance AS double), 2) FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.3, 0.0, 0.0, 0.0], k => 1))",
                "SELECT BIGINT '0', DOUBLE '0.09'");
    }

    @Test
    public void testResidualWhereFilter()
    {
        // WHERE above the TVF is applied to the k ANN results (post-filter semantics)
        assertQueryWithSameQueryRunner(
                "SELECT id FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.3, 0.0, 0.0, 0.0], k => 4)) WHERE tag = 'even' ORDER BY _distance",
                "VALUES BIGINT '0', BIGINT '2'");
    }

    @Test
    public void testUnknownTableFails()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'nope', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.0], k => 1))",
                ".*does not exist.*");
    }

    @Test
    public void testDimensionMismatchFails()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[1.0, 2.0], k => 1))",
                ".*dimension.*");
    }

    @Test
    public void testExplainContainsTopN()
    {
        // When all columns are projected (SELECT *), _distance is in the scan output,
        // so the TopN rewriter fires and inserts TopN above the scan.
        MaterializedResult result = computeActual(
                "EXPLAIN SELECT * FROM TABLE(lance.system.vector_search(" +
                        "schema_name => 'default', table_name => 'vectors', vector_col => 'vec', " +
                        "query_vector => ARRAY[0.3, 0.0, 0.0, 0.0], k => 3))");
        String plan = (String) result.getOnlyValue();
        assertTrue(plan.contains("TopN"), "EXPLAIN plan must contain TopN node proving the rewriter fired");
    }
}
