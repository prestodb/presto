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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestMySqlViewQuery
{
    private static final String CATALOG = "mysql";
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testCatalogRemovedWithoutSessionCatalog()
    {
        assertRewrite(
                "SELECT orderkey FROM mysql.tpch.orders",
                Optional.empty(),
                Optional.empty(),
                "SELECT orderkey FROM tpch.orders",
                Optional.empty());
    }

    @Test
    public void testDelimitedCatalogRemoved()
    {
        assertRewrite(
                "SELECT orderkey FROM \"mysql\".tpch.orders",
                Optional.empty(),
                Optional.empty(),
                "SELECT orderkey FROM tpch.orders",
                Optional.empty());
        assertRewrite(
                "SELECT orderkey FROM \"MySQL\".tpch.orders",
                Optional.empty(),
                Optional.empty(),
                "SELECT orderkey FROM tpch.orders",
                Optional.empty());
    }

    @Test
    public void testEveryTableNameRewritten()
    {
        // names in a join, a subquery in FROM, a subquery in WHERE and a WITH query, spread over
        // several lines of the formatted query, have to be spliced at their own offsets
        assertRewrite(
                "WITH big AS (SELECT * FROM mysql.tpch.orders WHERE totalprice > 1.5) " +
                        "SELECT o.orderkey FROM mysql.tpch.orders o " +
                        "JOIN (SELECT custkey FROM mysql.tpch.customer) c ON o.custkey = c.custkey " +
                        "JOIN big b ON o.orderkey = b.orderkey " +
                        "WHERE o.custkey IN (SELECT custkey FROM tpch.customer)",
                Optional.of(CATALOG),
                Optional.of("tpch"),
                "WITH big AS (SELECT * FROM tpch.orders WHERE totalprice > 1.5) " +
                        "SELECT o.orderkey FROM tpch.orders o " +
                        "JOIN (SELECT custkey FROM tpch.customer) c ON o.custkey = c.custkey " +
                        "JOIN big b ON o.orderkey = b.orderkey " +
                        "WHERE o.custkey IN (SELECT custkey FROM tpch.customer)",
                Optional.empty());
    }

    @Test
    public void testSchemaQualifiedNameUnchanged()
    {
        assertRewrite(
                "SELECT orderkey FROM tpch.orders",
                Optional.of(CATALOG),
                Optional.of("other"),
                "SELECT orderkey FROM tpch.orders",
                Optional.empty());
    }

    @Test
    public void testUnqualifiedNameUsesSessionSchema()
    {
        assertRewrite(
                "SELECT orderkey FROM orders",
                Optional.of(CATALOG),
                Optional.of("tpch"),
                "SELECT orderkey FROM orders",
                Optional.of("tpch"));
    }

    @Test
    public void testWithQueryNameIsNotATable()
    {
        // without a session catalog, a name without one can only be the WITH query
        assertRewrite(
                "WITH t AS (SELECT orderkey FROM mysql.tpch.orders) SELECT orderkey FROM t",
                Optional.empty(),
                Optional.empty(),
                "WITH t AS (SELECT orderkey FROM tpch.orders) SELECT orderkey FROM t",
                Optional.empty());
    }

    @Test
    public void testTableInAnotherCatalogRejected()
    {
        assertNotSupported("SELECT orderkey FROM tpch.tiny.orders", Optional.empty());
        assertNotSupported("SELECT orderkey FROM mysql.tpch.orders JOIN tpch.tiny.customer USING (custkey)", Optional.of(CATALOG));
        // the analyzer resolved these against the session catalog, which is not the view's
        assertNotSupported("SELECT orderkey FROM tiny.orders", Optional.of("tpch"));
        assertNotSupported("SELECT orderkey FROM orders", Optional.of("tpch"));
    }

    private static void assertRewrite(String sql, Optional<String> sessionCatalog, Optional<String> sessionSchema, String expectedSql, Optional<String> expectedDefaultSchema)
    {
        MySqlViewQuery viewQuery = MySqlViewQuery.rewrite(formatted(sql), CATALOG, sessionCatalog, sessionSchema);
        assertEquals(viewQuery.getSql(), formatted(expectedSql));
        assertEquals(viewQuery.getDefaultSchema(), expectedDefaultSchema);
    }

    private static void assertNotSupported(String sql, Optional<String> sessionCatalog)
    {
        assertThatThrownBy(() -> MySqlViewQuery.rewrite(formatted(sql), CATALOG, sessionCatalog, Optional.of("tiny")))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("A view in MySQL catalog 'mysql' can only read tables in that catalog, but its query reads '.*' from catalog 'tpch'")
                .satisfies(e -> assertEquals(((PrestoException) e).getErrorCode(), NOT_SUPPORTED.toErrorCode()));
    }

    // the view query as CreateViewTask records it
    private static String formatted(String sql)
    {
        Query query = (Query) SQL_PARSER.createStatement(sql, ParsingOptions.builder().setDecimalLiteralTreatment(AS_DECIMAL).build());
        return formatSql(query, Optional.empty());
    }
}
