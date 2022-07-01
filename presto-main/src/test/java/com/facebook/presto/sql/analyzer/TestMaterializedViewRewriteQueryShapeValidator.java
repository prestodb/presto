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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaterializedViewRewriteQueryShapeValidator
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void supportedFunction()
    {
        assertSucceeds("SELECT SUM(x) AS sum_x, y FROM tbl GROUP BY y ORDER BY z");
    }
    @Test
    public void unsupportedFunction()
    {
        assertFails(
                "SELECT AVG(x) AS avg_x, y FROM tbl GROUP BY y",
                "Query shape invalid: avg function is not supported for materialized view optimizations");
    }

    @Test
    public void havingClause()
    {
        assertFails(
                "SELECT SUM(x) AS sum_x, y FROM tbl GROUP BY y HAVING COUNT(x) < 10",
                "Query shape invalid: HAVING is not supported for materialized view optimizations");
    }

    private static void assertFails(String baseQuerySql, String expectedErrorMessage)
    {
        QuerySpecification querySpecification = (QuerySpecification) ((Query) SQL_PARSER.createStatement(baseQuerySql)).getQueryBody();
        Optional<String> errorMessage = MaterializedViewRewriteQueryShapeValidator.validate(querySpecification);
        assertTrue(errorMessage.isPresent());
        assertEquals(errorMessage.get(), expectedErrorMessage);
    }

    private static void assertSucceeds(String baseQuerySql)
    {
        QuerySpecification querySpecification = (QuerySpecification) ((Query) SQL_PARSER.createStatement(baseQuerySql)).getQueryBody();
        Optional<String> errorMessage = MaterializedViewRewriteQueryShapeValidator.validate(querySpecification);
        assertFalse(errorMessage.isPresent());
    }
}
