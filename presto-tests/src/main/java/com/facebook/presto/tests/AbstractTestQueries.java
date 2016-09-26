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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.facebook.presto.type.SqlIntervalYearMonth;
import com.facebook.presto.util.DateTimeZoneIndex;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import io.airlift.tpch.TpchTable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.tableNameGetter;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueries
        extends AbstractTestQueryFramework
{
    // We can just use the default type registry, since we don't use any parametric types
    protected static final List<SqlFunction> CUSTOM_FUNCTIONS = new FunctionListBuilder()
            .aggregate(CustomSum.class)
            .window(CustomRank.class)
            .scalars(CustomAdd.class)
            .scalars(CreateHll.class)
            .getFunctions();

    public static final List<PropertyMetadata<?>> TEST_SYSTEM_PROPERTIES = ImmutableList.of(
            PropertyMetadata.stringSessionProperty(
                    "test_string",
                    "test string property",
                    "test default",
                    false),
            PropertyMetadata.longSessionProperty(
                    "test_long",
                    "test long property",
                    42L,
                    false));
    public static final List<PropertyMetadata<?>> TEST_CATALOG_PROPERTIES = ImmutableList.of(
            PropertyMetadata.stringSessionProperty(
                    "connector_string",
                    "connector string property",
                    "connector default",
                    false),
            PropertyMetadata.longSessionProperty(
                    "connector_long",
                    "connector long property",
                    33L,
                    false),
            PropertyMetadata.booleanSessionProperty(
                    "connector_boolean",
                    "connector boolean property",
                    true,
                    false),
            PropertyMetadata.doubleSessionProperty(
                    "connector_double",
                    "connector double property",
                    99.0,
                    false));

    protected AbstractTestQueries(QueryRunner queryRunner)
    {
        super(queryRunner);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "line 1:16: no viable alternative at input.*")
    public void testParsingError()
            throws Exception
    {
        computeActual("SELECT foo FROM");
    }

    @Test
    public void selectLargeInterval()
            throws Exception
    {
        MaterializedResult result = computeActual("SELECT INTERVAL '30' DAY");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new SqlIntervalDayTime(30, 0, 0, 0, 0));

        result = computeActual("SELECT INTERVAL '" + Short.MAX_VALUE + "' YEAR");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new SqlIntervalYearMonth(Short.MAX_VALUE, 0));
    }

    @Test
    public void selectNull()
            throws Exception
    {
        assertQuery("SELECT NULL", "SELECT NULL FROM (SELECT * FROM ORDERS LIMIT 1)");
    }

    @Test
    public void testLimitIntMax()
            throws Exception
    {
        assertQuery("SELECT orderkey from orders LIMIT " + Integer.MAX_VALUE);
        assertQuery("SELECT orderkey from orders ORDER BY orderkey LIMIT " + Integer.MAX_VALUE);
    }

    @Test
    public void testNonDeterministicFilter()
    {
        MaterializedResult materializedResult = computeActual("SELECT u FROM ( SELECT if(rand() > 0.5, 0, 1) AS u ) WHERE u <> u");
        assertEquals(materializedResult.getRowCount(), 0);

        materializedResult = computeActual("SELECT u, v FROM ( SELECT if(rand() > 0.5, 0, 1) AS u, 4*4 as v ) WHERE u <> u and v > 10");
        assertEquals(materializedResult.getRowCount(), 0);

        materializedResult = computeActual("SELECT u, v, w FROM ( SELECT if(rand() > 0.5, 0, 1) AS u, 4*4 as v, 'abc' as w ) WHERE v > 10");
        assertEquals(materializedResult.getRowCount(), 1);
    }

    @Test
    public void testNonDeterministicProjection()
    {
        MaterializedResult materializedResult = computeActual("select r, r + 1 from (select rand(100) r from orders) limit 10");
        assertEquals(materializedResult.getRowCount(), 10);
        for (MaterializedRow materializedRow : materializedResult) {
            assertEquals(materializedRow.getFieldCount(), 2);
            assertEquals(((Number) materializedRow.getField(0)).intValue() + 1, materializedRow.getField(1));
        }
    }

    @Test
    public void testMapSubscript()
            throws Exception
    {
        assertQuery("select map(array[1], array['aa'])[1]", "select 'aa'");
        assertQuery("select map(array['a'], array['aa'])['a']", "select 'aa'");
        assertQuery("select map(array[array[1,1]], array['a'])[array[1,1]]", "select 'a'");
        assertQuery("select map(array[(1,2)], array['a'])[(1,2)]", "select 'a'");
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        assertQuery("SELECT LENGTH(x) FROM (SELECT from_base64('gw==') as x)", "SELECT 1");
        assertQuery("SELECT LENGTH(from_base64('gw=='))", "SELECT 1");
    }

    @Test
    public void testRowFieldAccessor()
            throws Exception
    {
        //Dereference only
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(1.0, 2.0) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1.0");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(TRUE, FALSE) AS ROW(col0 boolean, col1 boolean)))) AS t (a)", "SELECT TRUE");
        assertQuery("SELECT a.col1 FROM (VALUES ROW (CAST(ROW(1.0, 'kittens') AS ROW(col0 varchar, col1 varchar)))) AS t (a)", "SELECT 'kittens'");
        assertQuery("SELECT a.col2.col1 FROM (VALUES ROW(CAST(ROW(1.0, ARRAY[2], row(3, 4.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 4.0");

        // mixture of row field reference and table field reference
        assertQuery("SELECT cast(row(1, t.x) as row(col0 bigint, col1 bigint)).col1 FROM (VALUES 1, 2, 3) t(x)", "SELECT * FROM (VALUES 1, 2, 3)");
        assertQuery("SELECT Y.col1 FROM (SELECT cast(row(1, t.x) as row(col0 bigint, col1 bigint)) AS Y FROM (VALUES 1, 2, 3) t(x)) test_t", "SELECT * FROM (VALUES 1, 2, 3)");

        // Subscript + Dereference
        assertQuery("SELECT a.col1[2] FROM (VALUES ROW(CAST(ROW(1.0, ARRAY[22, 33, 44, 55], row(3, 4.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 33");
        assertQuery("SELECT a.col1[2].col0, a.col1[2].col1 FROM (VALUES ROW(cast(row(1.0, ARRAY[row(31, 4.1), row(32, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 32, 4.2");

        assertQuery("SELECT cast(row(11, 12) as row(col0 bigint, col1 bigint)).col0", "SELECT 11");
    }

    @Test
    public void testRowFieldAccessorInAggregate()
            throws Exception
    {
        assertQuery("SELECT a.col0, SUM(a.col1[2]), SUM(a.col2.col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(12, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col0",
                "SELECT * FROM VALUES (1.0, 46, 24, 9.1), (2.0, 23, 12, 14.0)");

        assertQuery("SELECT a.col2.col0, SUM(a.col0), SUM(a.col1[2]), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(11, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col2.col0",
                "SELECT * FROM VALUES (11, 3.0, 36, 18.1), (13, 7.0, 33, 5.0)");

        assertQuery("SELECT a.col1[1].col0, SUM(a.col0), SUM(a.col1[1].col1), SUM(a.col1[2].col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5), row(12, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2), row(22, 4.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col1[1].col0",
                "SELECT * FROM VALUES (31, 3.2, 8.7, 34, 8.0), (41, 3.1, 3.1, 32, 6.0)");

        assertQuery("SELECT a.col1[1].col0, SUM(a.col0), SUM(a.col1[1].col1), SUM(a.col1[2].col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2), row(22, 4.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5), row(12, 4.2)], row(3, 4.1)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.3, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))) " +
                        ") t(a) " +
                        "GROUP BY a.col1[1]",
                "SELECT * FROM VALUES (31, 2.2, 4.2, 22, 4.0), (31, 1.0, 4.5, 12, 4.1), (41, 6.4, 6.2, 64, 12.0)");

        assertQuery("SELECT a.col1[2], SUM(a.col0), SUM(a.col1[1]), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 13, 4], row(12, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col1[2]",
                "SELECT * FROM VALUES (13, 3.0, 4, 18.1), (33, 7.0, 22, 5.0)");

        assertQuery("SELECT a.col2.col0, SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2), row(22, 4.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5), row(12, 4.2)], row(3, 4.1)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.3, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))) " +
                        ") t(a) " +
                        "GROUP BY a.col2",
                "SELECT * FROM VALUES (5, 4.0), (3, 4.1), (6, 12.0)");

        assertQuery("SELECT a.col2.col0, a.col0, SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(11, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.5, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.5, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "WHERE a.col1[2] < 30 " +
                        "GROUP BY 1, 2 ORDER BY 1",
                "SELECT * FROM VALUES (11, 1.0, 4.1), (11, 1.5, 8.2), (11, 2.0, 14.0)");

        assertQuery("SELECT a[1].col0, COUNT(1) FROM " +
                        "(VALUES " +
                        "(ROW(CAST(ARRAY[row(31, 4.2), row(22, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(31, 4.5), row(12, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(41, 3.1), row(32, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(31, 3.1), row(32, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))) " +
                        ") t(a) " +
                        "GROUP BY 1 " +
                        "ORDER BY 2 DESC",
                "SELECT * FROM VALUES (31, 3), (41, 1)");
    }

    @Test
    public void testRowFieldAccessorInWindowFunction()
            throws Exception
    {
        assertQuery("SELECT a.col0, " +
                        "SUM(a.col1[1].col1) OVER(PARTITION BY a.col2.col0), " +
                        "SUM(a.col2.col1) OVER(PARTITION BY a.col2.col0) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 14.5), row(12, 4.2)], row(3, 4.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(41, 17.1), row(45, 4.2)], row(7, 16.0)) AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) ",
                "SELECT * FROM VALUES (1.0, 14.5, 4.0), (2.2, 39.3, 18.0), (2.2, 39.3, 18.0), (2.2, 17.1, 16.0), (3.1, 39.3, 18.0)");

        assertQuery("SELECT a.col1[1].col0, " +
                        "SUM(a.col0) OVER(PARTITION BY a.col1[1].col0), " +
                        "SUM(a.col1[1].col1) OVER(PARTITION BY a.col1[1].col0), " +
                        "SUM(a.col2.col1) OVER(PARTITION BY a.col1[1].col0) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 14.5), row(12, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 14.2), row(22, 5.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) " +
                        "WHERE a.col1[2].col1 > a.col2.col0",
                "SELECT * FROM VALUES (31, 3.2, 28.7, 8.0), (31, 3.2, 28.7, 8.0)");
    }

    @Test
    public void testRowFieldAccessorInJoin()
            throws Exception
    {
        assertQuery("" +
                        "SELECT t.a.col1, custkey, orderkey FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1, 11) AS ROW(col0 integer, col1 integer))), " +
                        "ROW(CAST(ROW(2, 22) AS ROW(col0 integer, col1 integer))), " +
                        "ROW(CAST(ROW(3, 33) AS ROW(col0 integer, col1 integer)))) t(a) " +
                        "INNER JOIN orders " +
                        "ON t.a.col0 = orders.orderkey",
                "SELECT * FROM VALUES (11, 370, 1), (22, 781, 2), (33, 1234, 3)");
    }

    @Test
    public void testRowCast()
            throws Exception
    {
        assertQuery("SELECT cast(row(1, 2) as row(aa bigint, bb boolean)).aa", "SELECT 1");
        assertQuery("SELECT cast(row(1, 2) as row(aa bigint, bb boolean)).bb", "SELECT true");
        assertQuery("SELECT cast(row(1, 2) as row(aa bigint, bb varchar)).bb", "SELECT '2'");
        assertQuery("SELECT cast(row(true, array[0, 2]) as row(aa boolean, bb array(boolean))).bb[1]", "SELECT false");
        assertQuery("SELECT cast(row(0.1, array[0, 2], row(1, 0.5)) as row(aa bigint, bb array(boolean), cc row(dd varchar, ee varchar))).cc.ee", "SELECT '0.5'");
        assertQuery("SELECT cast(array[row(0.1, array[0, 2], row(1, 0.5))] as array<row(aa bigint, bb array(boolean), cc row(dd varchar, ee varchar))>)[1].cc.ee", "SELECT '0.5'");
    }

    @Test
    public void testDereferenceInSubquery()
            throws Exception
    {
        assertQuery("" +
                        "SELECT x " +
                        "FROM (" +
                        "   SELECT a.x" +
                        "   FROM (VALUES 1, 2, 3) a(x)" +
                        ") " +
                        "GROUP BY x",
                "SELECT * FROM VALUES 1, 2, 3");

        assertQuery("" +
                        "SELECT t2.*, max(t1.b) as max_b " +
                        "FROM (VALUES (1, 'a'),  (2, 'b'), (1, 'c'), (3, 'd')) t1(a, b) " +
                        "INNER JOIN " +
                        "(VALUES 1, 2, 3, 4) t2(a) " +
                        "ON t1.a = t2.a " +
                        "GROUP BY t2.a",
                "SELECT * FROM VALUES (1, 'c'), (2, 'b'), (3, 'd')");

        assertQuery("" +
                        "SELECT t2.*, max(t1.b1) as max_b1 " +
                        "FROM (VALUES (1, 'a'),  (2, 'b'), (1, 'c'), (3, 'd')) t1(a1, b1) " +
                        "INNER JOIN " +
                        "(VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333), (4, 44, 444)) t2(a2, b2, c2) " +
                        "ON t1.a1 = t2.a2 " +
                        "GROUP BY t2.a2, t2.b2, t2.c2",
                "SELECT * FROM VALUES (1, 11, 111, 'c'), (2, 22, 222, 'b'), (3, 33, 333, 'd')");

        assertQuery("" +
                "SELECT custkey, orders2 " +
                "FROM (" +
                "   SELECT x.custkey, SUM(x.orders) + 1 orders2 " +
                "   FROM ( " +
                "      SELECT x.custkey, COUNT(x.orderkey) orders " +
                "      FROM ORDERS x " +
                "      WHERE x.custkey < 100 " +
                "      GROUP BY x.custkey " +
                "   ) x " +
                "   GROUP BY x.custkey" +
                ") " +
                "ORDER BY custkey");
    }

    @Test
    public void testDereferenceInFunctionCall()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(DISTINCT custkey) " +
                "FROM ( " +
                "  SELECT x.custkey " +
                "  FROM ORDERS x " +
                "  WHERE custkey < 100 " +
                ") t");
    }

    @Test
    public void testDereferenceInComparsion()
            throws Exception
    {
        assertQuery("" +
                "SELECT orders.custkey, orders.orderkey " +
                "FROM ORDERS " +
                "WHERE orders.custkey > orders.orderkey AND orders.custkey < 200.3");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "line 1:8: '\"a\".\"col0\"' must be an aggregate expression or appear in GROUP BY clause")
    public void testMissingRowFieldInGroupBy()
            throws Exception
    {
        assertQuery("SELECT a.col0, count(*) FROM (VALUES ROW(cast(ROW(1, 1) as ROW(col0 integer, col1 integer)))) t(a)");
    }

    @Test
    public void testWhereWithRowField()
            throws Exception
    {
        assertQuery("SELECT a.col0 FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 > 0", "SELECT 1");
        assertQuery("SELECT SUM(a.col0) FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 <= 0", "SELECT null");

        assertQuery("SELECT a.col0 FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 < a.col1", "SELECT 1");
        assertQuery("SELECT SUM(a.col0) FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 < a.col1", "SELECT 1");
        assertQuery("SELECT SUM(a.col0) FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 > a.col1", "SELECT null");
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a)", "SELECT 1");
        assertQuery("SELECT x[1] FROM UNNEST(ARRAY[ARRAY[1, 2, 3]]) t(x)", "SELECT 1");
        assertQuery("SELECT x[1][2] FROM UNNEST(ARRAY[ARRAY[ARRAY[1, 2, 3]]]) t(x)", "SELECT 2");
        assertQuery("SELECT x[2] FROM UNNEST(ARRAY[MAP(ARRAY[1,2], ARRAY['hello', 'hi'])]) t(x)", "SELECT 'hi'");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3])", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3]) t(a)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4]) t(a, b)", "SELECT * FROM VALUES (1, 3), (2, 4)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES (1, 4), (2, 5), (3, NULL)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 1, 2, 3");
        assertQuery("SELECT b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 4, 5, NULL");
        assertQuery("SELECT count(*) FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5])", "SELECT 3");
        assertQuery("SELECT a FROM UNNEST(ARRAY['kittens', 'puppies']) t(a)", "SELECT * FROM VALUES ('kittens'), ('puppies')");
        assertQuery("" +
                "SELECT c " +
                "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b) " +
                "CROSS JOIN (values (8), (9)) t2(c)",
                "SELECT * FROM VALUES 8, 8, 8, 9, 9, 9");
        assertQuery("" +
                "SELECT a.custkey, t.e " +
                "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a " +
                "CROSS JOIN UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), (3))");
        assertQuery("" +
                        "SELECT a.custkey, t.e " +
                        "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, " +
                        "UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), (3))");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', 'dog'])) t(a, b)", "SELECT * FROM VALUES (1, 'cat'), (2, 'dog')");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', NULL])) t(a, b)", "SELECT * FROM VALUES (1, 'cat'), (2, NULL)");

        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a) WITH ORDINALITY", "SELECT 1");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY", "SELECT * FROM VALUES (1, 1), (2, 2), (3, 3)");
        assertQuery("SELECT b FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b, c FROM UNNEST(ARRAY[10, 20, 30], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c)", "SELECT * FROM VALUES (10, 4, 1), (20, 5, 2), (30, NULL, 3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY['kittens', 'puppies']) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES ('kittens', 1), ('puppies', 2)");
        assertQuery("" +
                        "SELECT c " +
                        "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c) " +
                        "CROSS JOIN (values (8), (9)) t2(d)",
                "SELECT * FROM VALUES 1, 1, 2, 2, 3, 3");
        assertQuery("" +
                        "SELECT a.custkey, t.e, t.f " +
                        "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a " +
                        "CROSS JOIN UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), (20, 2), (30, 3))");
        assertQuery("" +
                        "SELECT a.custkey, t.e, t.f " +
                        "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, " +
                        "UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), (20, 2), (30, 3))");

        assertQuery("SELECT * FROM orders, UNNEST(ARRAY[1])", "SELECT orders.*, 1 FROM orders");
    }

    @Test
    public void testArrays()
            throws Exception
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1 + cast(round(rand()) AS BIGINT)] FROM (SELECT ARRAY[orderkey, orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1] + 1 FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1][1] FROM (SELECT ARRAY[ARRAY[orderkey + 1]] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT CARDINALITY(a) FROM (SELECT ARRAY[orderkey, orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT 2 FROM orders");
    }

    @Test
    public void testRows()
            throws Exception
    {
        // Using JSON_FORMAT(CAST(_ AS JSON)) because H2 does not support ROW type
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))", "SELECT '[3,\"ab\"]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(a + b) AS JSON)) FROM (VALUES (1, 2)) AS t(a, b)", "SELECT '[3]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(1, ROW(9, a, ARRAY[], NULL), ROW(1, 2)) AS JSON)) FROM (VALUES ('a')) t(a)", "SELECT '[1,[9,\"a\",[],null],[1,2]]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(ROW(ROW(ROW(ROW(a, b), c), d), e), f) AS JSON)) FROM (VALUES (ROW(0, 1), 2, '3', NULL, ARRAY[5], ARRAY[])) t(a, b, c, d, e, f)",
                "SELECT '[[[[[[0,1],2],\"3\"],null],[5]],[]]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(a, b)) AS JSON)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery("SELECT CONTAINS(ARRAY_AGG(ROW(a, b)), ROW(1, 2)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT TRUE");
        assertQuery("SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(c, d)) AS JSON)) FROM (VALUES (ARRAY[1, 3, 5], ARRAY[2, 4, 6])) AS t(a, b) CROSS JOIN UNNEST(a, b) AS u(c, d)",
                "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, NULL, '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, CAST(NULL AS INTEGER), '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
    }

    @Test
    public void testMaps()
            throws Exception
    {
        assertQuery("SELECT m[max_key] FROM (SELECT map_agg(orderkey, orderkey) m, max(orderkey) max_key FROM orders)", "SELECT max(orderkey) FROM orders");
    }

    @Test
    public void testValues()
            throws Exception
    {
        assertQuery("VALUES 1, 2, 3, 4");
        assertQuery("VALUES 1, 3, 2, 4 ORDER BY 1", "SELECT * FROM (VALUES 1, 3, 2, 4) ORDER BY 1");
        assertQuery("VALUES (1.1, 2, 'foo'), (sin(3.3), 2+2, 'bar')");
        assertQuery("VALUES (1.1, 2), (sin(3.3), 2+2) ORDER BY 1", "VALUES (sin(3.3), 2+2), (1.1, 2)");
        assertQuery("VALUES (1.1, 2), (sin(3.3), 2+2) LIMIT 1", "VALUES (1.1, 2)");
        assertQuery("SELECT * FROM (VALUES (1.1, 2), (sin(3.3), 2+2))");
        assertQuery(
                "SELECT * FROM (VALUES (1.1, 2), (sin(3.3), 2+2)) x (a, b) LEFT JOIN (VALUES (1.1, 2), (1.1, 2+2)) y (a, b) USING (a)",
                "VALUES (1.1, 2, 1.1, 4), (1.1, 2, 1.1, 2), (sin(3.3), 4, NULL, NULL)");
        assertQuery("SELECT 1.1 in (VALUES (1.1), (2.2))", "VALUES (TRUE)");

        assertQuery("" +
                "WITH a AS (VALUES (1.1, 2), (sin(3.3), 2+2)) " +
                "SELECT * FROM a",
                "VALUES (1.1, 2), (sin(3.3), 2+2)");

        // implicity coersions
        assertQuery("VALUES 1, 2.2, 3, 4.4");
        assertQuery("VALUES (1, 2), (3.3, 4.4)");
        assertQuery("VALUES true, 1.0 in (1, 2, 3)");
    }

    @Test
    public void testSpecialFloatingPointValues()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT nan(), infinity(), -infinity()");
        MaterializedRow row = getOnlyElement(actual.getMaterializedRows());
        assertEquals(row.getField(0), Double.NaN);
        assertEquals(row.getField(1), Double.POSITIVE_INFINITY);
        assertEquals(row.getField(2), Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testMaxMinStringWithNulls()
            throws Exception
    {
        assertQuery("SELECT custkey, MAX(NULLIF(orderstatus, 'O')), MIN(NULLIF(orderstatus, 'O')) FROM orders GROUP BY custkey");
    }

    @Test
    public void testApproxPercentile()
            throws Exception
    {
        MaterializedResult raw = computeActual("SELECT orderstatus, orderkey, totalprice FROM ORDERS");

        Multimap<String, Long> orderKeyByStatus = ArrayListMultimap.create();
        Multimap<String, Double> totalPriceByStatus = ArrayListMultimap.create();
        for (MaterializedRow row : raw.getMaterializedRows()) {
            orderKeyByStatus.put((String) row.getField(0), (Long) row.getField(1));
            totalPriceByStatus.put((String) row.getField(0), (Double) row.getField(2));
        }

        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, " +
                "   approx_percentile(orderkey, 0.5), " +
                "   approx_percentile(totalprice, 0.5)," +
                "   approx_percentile(orderkey, 2, 0.5)," +
                "   approx_percentile(totalprice, 2, 0.5)\n" +
                "FROM ORDERS\n" +
                "GROUP BY orderstatus");

        for (MaterializedRow row : actual.getMaterializedRows()) {
            String status = (String) row.getField(0);
            Long orderKey = (Long) row.getField(1);
            Double totalPrice = (Double) row.getField(2);
            Long orderKeyWeighted = (Long) row.getField(3);
            Double totalPriceWeighted = (Double) row.getField(4);

            List<Long> orderKeys = Ordering.natural().sortedCopy(orderKeyByStatus.get(status));
            List<Double> totalPrices = Ordering.natural().sortedCopy(totalPriceByStatus.get(status));

            // verify real rank of returned value is within 1% of requested rank
            assertTrue(orderKey >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKey <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(orderKeyWeighted >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKeyWeighted <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(totalPrice >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPrice <= totalPrices.get((int) (0.51 * totalPrices.size())));

            assertTrue(totalPriceWeighted >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPriceWeighted <= totalPrices.get((int) (0.51 * totalPrices.size())));
        }
    }

    @Test
    public void testComplexQuery()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT sum(orderkey), row_number() OVER (ORDER BY orderkey)\n" +
                "FROM orders\n" +
                "WHERE orderkey <= 10\n" +
                "GROUP BY orderkey\n" +
                "HAVING sum(orderkey) >= 3\n" +
                "ORDER BY orderkey DESC\n" +
                "LIMIT 3");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(7L, 5L)
                .row(6L, 4L)
                .row(5L, 3L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWhereNull()
            throws Exception
    {
        // This query is has this strange shape to force the compiler to leave a true on the stack
        // with the null flag set so if the filter method is not handling nulls correctly, this
        // query will fail
        assertQuery("SELECT custkey FROM orders WHERE custkey = custkey AND cast(nullif(custkey, custkey) as boolean) AND cast(nullif(custkey, custkey) as boolean)");
    }

    @Test
    public void testSumOfNulls()
            throws Exception
    {
        assertQuery("SELECT orderstatus, sum(CAST(NULL AS BIGINT)) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testAggregationWithSomeArgumentCasts()
            throws Exception
    {
        assertQuery("SELECT APPROX_PERCENTILE(0.1, x), AVG(x), MIN(x) FROM (values 1, 1, 1) t(x)", "SELECT 0.1, 1.0, 1");
    }

    @Test
    public void testAggregationWithHaving()
            throws Exception
    {
        assertQuery("SELECT a, count(1) FROM (VALUES 1, 2, 3, 2) t(a) GROUP BY a HAVING count(1) > 1", "SELECT 2, 2");
    }

    @Test
    public void testApproximateCountDistinct()
            throws Exception
    {
        assertQuery("SELECT approx_distinct(custkey) FROM orders", "SELECT 996");
        assertQuery("SELECT approx_distinct(custkey, 0.023) FROM orders", "SELECT 996");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE)) FROM orders", "SELECT 1031");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE), 0.023) FROM orders", "SELECT 1031");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR)) FROM orders", "SELECT 1011");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR), 0.023) FROM orders", "SELECT 1011");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR))) FROM orders", "SELECT 1011");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR)), 0.023) FROM orders", "SELECT 1011");
    }

    @Test
    public void testApproximateCountDistinctGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 995L)
                .row("F", 993L)
                .row("P", 303L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproximateCountDistinctGroupByWithStandardError()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey, 0.023) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 995L)
                .row("F", 993L)
                .row("P", 303L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCountBoolean()
            throws Exception
    {
        assertQuery("SELECT COUNT(true) FROM orders");
    }

    @Test
    public void testJoinWithMultiFieldGroupBy()
            throws Exception
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM ORDERS) T on lineitem.orderkey = T.orderkey");
    }

    @Test
    public void testGroupByRepeatedField()
            throws Exception
    {
        assertQuery("SELECT sum(custkey) FROM orders GROUP BY orderstatus, orderstatus");
    }

    @Test
    public void testGroupByRepeatedField2()
            throws Exception
    {
        assertQuery("SELECT count(*) FROM (select orderstatus a, orderstatus b FROM orders) GROUP BY a, b");
    }

    @Test
    public void testGroupByMultipleFieldsWithPredicateOnAggregationArgument()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, MAX(orderkey) FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus");
    }

    @Test
    public void testReorderOutputsOfGroupByAggregation()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, a, custkey, b FROM (SELECT custkey, orderstatus, -COUNT(*) a, MAX(orderkey) b FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus) T");
    }

    @Test
    public void testGroupAggregationOverNestedGroupByAggregation()
            throws Exception
    {
        assertQuery("SELECT sum(custkey), max(orderstatus), min(c) FROM (SELECT orderstatus, custkey, COUNT(*) c FROM ORDERS GROUP BY orderstatus, custkey) T");
    }

    @Test
    public void test15WayGroupBy()
            throws Exception
    {
        // Among other things, this test verifies we are not getting for overflow in the distributed HashPagePartitionFunction
        assertQuery("" +
                "SELECT " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10, " +
                "    count(*) " +
                "FROM orders " +
                "GROUP BY " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10");
    }

    @Test
    public void testDistinctMultipleFields()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM ORDERS");
    }

    @Test
    public void testDistinctJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT CAST(b.quantity AS BIGINT)), a.orderstatus " +
                "FROM orders a " +
                "JOIN lineitem b " +
                "ON a.orderkey = b.orderkey " +
                "GROUP BY a.orderstatus");
    }

    @Test
    public void testArithmeticNegation()
            throws Exception
    {
        assertQuery("SELECT -custkey FROM orders");
    }

    @Test
    public void testDistinct()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey FROM orders");
    }

    @Test
    public void testDistinctGroupBy()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
    }

    @Test
    public void testSingleDistinctOptimizer()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("" +
                "SELECT custkey, COUNT(DISTINCT orderstatus) FROM (" +
                "   SELECT orders.custkey AS custkey, orders.orderstatus AS orderstatus " +
                "   FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey " +
                "   GROUP BY orders.custkey, orders.orderstatus" +
                ") " +
                "GROUP BY custkey");
        assertQuery("SELECT custkey, COUNT(DISTINCT orderkey), COUNT(DISTINCT orderstatus) FROM orders GROUP BY custkey");

        assertQuery("SELECT SUM(DISTINCT x) FROM (SELECT custkey, COUNT(DISTINCT orderstatus) x FROM orders GROUP BY custkey) t");
    }

    @Test
    public void testDistinctHaving()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "HAVING COUNT(DISTINCT clerk) > 1");
    }

    @Test
    public void testDistinctWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual(
                "SELECT RANK() OVER (PARTITION BY orderdate ORDER BY COUNT(DISTINCT clerk)) rnk " +
                "FROM orders " +
                "GROUP BY orderdate, custkey " +
                "ORDER BY rnk " +
                "LIMIT 1");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT).row(1L).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDistinctWhere()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) FROM orders WHERE LENGTH(clerk) > 5");
    }

    @Test
    public void testMultipleDifferentDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT orderstatus), SUM(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testMultipleDistinct()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), SUM(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testComplexDistinct()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey + 1.0), " +
                        "AVG(DISTINCT custkey), " +
                        "VARIANCE(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), " +
                        "SUM(custkey), " +
                        "SUM(custkey + 1.0), " +
                        "AVG(custkey), " +
                        "VARIANCE(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testDistinctLimit()
            throws Exception
    {
        assertQuery("" +
                "SELECT DISTINCT orderstatus, custkey " +
                "FROM (SELECT orderstatus, custkey FROM orders ORDER BY orderkey LIMIT 10) " +
                "LIMIT 10");
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");
    }

    @Test
    public void testCountDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
    }

    @Test
    public void testDistinctWithOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 10");
    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "line 1:1: For SELECT DISTINCT, ORDER BY expressions must appear in select list")
    public void testDistinctWithOrderByNotInSelect()
            throws Exception
    {
        assertQueryOrdered("SELECT DISTINCT custkey FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Test
    public void testOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey DESC LIMIT 10");
    }

    @Test
    public void testOrderByExpressionWithLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 DESC LIMIT 10");
    }

    @Test
    public void testGroupByOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey ORDER BY SUM(totalprice) DESC LIMIT 10");
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders LIMIT 0");
    }

    @Test
    public void testLimitAll()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders LIMIT ALL", "SELECT custkey, totalprice FROM orders");
    }

    @Test
    public void testOrderByLimitZero()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT 0");
    }

    @Test
    public void testOrderByLimitAll()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT ALL", "SELECT custkey, totalprice FROM orders ORDER BY orderkey");
    }

    @Test
    public void testRepeatedAggregations()
            throws Exception
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM ORDERS");
    }

    @Test
    public void testRepeatedOutputs()
            throws Exception
    {
        assertQuery("SELECT orderkey a, orderkey b FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testRepeatedOutputs2()
            throws Exception
    {
        // this test exposed a bug that wasn't caught by other tests that resulted in the execution engine
        // trying to read orderkey as the second field, causing a type mismatch
        assertQuery("SELECT orderdate, orderdate, orderkey FROM orders");
    }

    @Test
    public void testLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM ORDERS LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testAggregationWithLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitInInlineView()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM ORDERS LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testCountAll()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42) FROM ORDERS", "SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42 + 42) FROM ORDERS", "SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(null) FROM ORDERS", "SELECT 0");
    }

    @Test
    public void testCountColumn()
            throws Exception
    {
        assertQuery("SELECT COUNT(orderkey) FROM ORDERS");
        assertQuery("SELECT COUNT(orderstatus) FROM ORDERS");
        assertQuery("SELECT COUNT(orderdate) FROM ORDERS");
        assertQuery("SELECT COUNT(1) FROM ORDERS");

        assertQuery("SELECT COUNT(NULLIF(orderstatus, 'F')) FROM ORDERS");
        assertQuery("SELECT COUNT(CAST(NULL AS BIGINT)) FROM ORDERS"); // todo: make COUNT(null) work
    }

    @Test
    public void testWildcard()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS");
    }

    @Test
    public void testMultipleWildcards()
            throws Exception
    {
        assertQuery("SELECT *, 123, * FROM ORDERS");
    }

    @Test
    public void testMixedWildcards()
            throws Exception
    {
        assertQuery("SELECT *, orders.*, orderkey FROM orders");
    }

    @Test
    public void testQualifiedWildcardFromAlias()
            throws Exception
    {
        assertQuery("SELECT T.* FROM ORDERS T");
    }

    @Test
    public void testQualifiedWildcardFromInlineView()
            throws Exception
    {
        assertQuery("SELECT T.* FROM (SELECT orderkey + custkey FROM ORDERS) T");
    }

    @Test
    public void testQualifiedWildcard()
            throws Exception
    {
        assertQuery("SELECT ORDERS.* FROM ORDERS");
    }

    @Test
    public void testAverageAll()
            throws Exception
    {
        assertQuery("SELECT AVG(totalprice) FROM ORDERS");
    }

    @Test
    public void testVariance()
            throws Exception
    {
        // int64
        assertQuery("SELECT VAR_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testVariancePop()
            throws Exception
    {
        // int64
        assertQuery("SELECT VAR_POP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDev()
            throws Exception
    {
        // int64
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDevPop()
            throws Exception
    {
        // int64
        assertQuery("SELECT STDDEV_POP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testCountAllWithPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testGroupByArray()
            throws Exception
    {
        assertQuery("SELECT col[1], count FROM (SELECT ARRAY[custkey] col, COUNT(*) count FROM ORDERS GROUP BY 1 ORDER BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey ORDER BY custkey");
    }

    @Test
    public void testGroupByMap()
            throws Exception
    {
        assertQuery("SELECT col[1], count FROM (SELECT MAP(ARRAY[1], ARRAY[custkey]) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByComplexMap()
            throws Exception
    {
        assertQuery("SELECT MAP_KEYS(x)[1] FROM (VALUES MAP(ARRAY['a'], ARRAY[ARRAY[1]]), MAP(ARRAY['b'], ARRAY[ARRAY[2]])) t(x) GROUP BY x", "SELECT * FROM (VALUES 'a', 'b')");
    }

    @Test
    public void testGroupByRow()
            throws Exception
    {
        assertQuery("SELECT col.col1, count FROM (SELECT cast(row(custkey, custkey) as row(col0 bigint, col1 bigint)) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testJoinCoercion()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders t join (SELECT * FROM orders LIMIT 1) t2 ON sin(t2.custkey) = 0");
    }

    @Test
    public void testJoinCoercionOnEqualityComparison()
            throws Exception
    {
        assertQuery("SELECT o.clerk, avg(o.shippriority), COUNT(l.linenumber) FROM orders o LEFT OUTER JOIN lineitem l ON o.orderkey=l.orderkey AND o.shippriority=1 GROUP BY o.clerk");
    }

    @Test
    public void testGroupByNoAggregations()
            throws Exception
    {
        assertQuery("SELECT custkey FROM ORDERS GROUP BY custkey");
    }

    @Test
    public void testGroupByCount()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, COUNT(*) FROM ORDERS GROUP BY orderstatus",
                "SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus"
        );
    }

    @Test
    public void testGroupByMultipleFields()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(*) FROM ORDERS GROUP BY custkey, orderstatus");
    }

    @Test
    public void testGroupByWithAlias()
            throws Exception
    {
        assertQuery(
                "SELECT orderdate x, COUNT(*) FROM orders GROUP BY orderdate",
                "SELECT orderdate x, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderdate"
        );
    }

    @Test
    public void testGroupBySum()
            throws Exception
    {
        assertQuery("SELECT suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupByRequireIntegerCoercion()
            throws Exception
    {
        assertQuery("SELECT partkey, COUNT(DISTINCT shipdate), SUM(linenumber) FROM lineitem GROUP BY partkey");
    }

    @Test
    public void testGroupByEmptyGroupingSet()
            throws Exception
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY ()",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupByWithWildcard()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT orderkey FROM orders) t GROUP BY orderkey");
    }

    @Test
    public void testSingleGroupingSet()
            throws Exception
    {
        assertQuery(
                "SELECT linenumber, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "GROUP BY GROUPING SETS (linenumber)",
                "SELECT linenumber, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "GROUP BY linenumber");
    }

    @Test
    public void testGroupingSets()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithSingleDistinct()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinct()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsGrandTotalSet()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsRepeatedSetsAll()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem UNION ALL " +
                        "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsRepeatedSetsDistinct()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY DISTINCT GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsGrandTotalSetFirst()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), (linenumber), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsOnlyGrandTotalSet()
            throws Exception
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS (())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleGrandTotalSets()
            throws Exception
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), ())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem UNION ALL " +
                        "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetMixedExpressionAndColumn()
            throws Exception
    {
        assertQuery("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), ROLLUP(suppkey)",
                "SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), suppkey UNION ALL " +
                        "SELECT NULL, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate)");
    }

    @Test
    public void testGroupingSetMixedExpressionAndOrdinal()
            throws Exception
    {
        assertQuery("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY 2, ROLLUP(suppkey)",
                "SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), suppkey UNION ALL " +
                        "SELECT NULL, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate)");
    }

    @Test
    public void testGroupingSetSubsetAndPartitioning()
            throws Exception
    {
        assertQuery("SELECT COUNT_IF(x IS NULL) FROM (" +
                        "SELECT x, y, COUNT(z) FROM (SELECT CAST(lineitem.orderkey AS BIGINT) x, lineitem.linestatus y, SUM(lineitem.quantity) z FROM lineitem " +
                        "JOIN orders ON lineitem.orderkey = orders.orderkey GROUP BY 1, 2) GROUP BY GROUPING SETS ((x, y), ()))",
                "SELECT 1");
    }

    @Test
    public void testGroupingSetPredicatePushdown()
            throws Exception
    {
        assertQuery("SELECT * FROM (" +
                        "SELECT COALESCE(orderpriority, 'ALL'), COALESCE(shippriority, -1) sp FROM (" +
                        "SELECT orderpriority, shippriority, COUNT(1) FROM orders GROUP BY GROUPING SETS ((orderpriority), (shippriority)))) WHERE sp=-1",
                "SELECT orderpriority, -1 FROM orders GROUP BY orderpriority");
    }

    @Test
    public void testGroupingSetsAggregateOnGroupedColumn()
            throws Exception
    {
        assertQuery("SELECT orderpriority, COUNT(orderpriority) FROM orders GROUP BY ROLLUP (orderpriority)",
                "SELECT orderpriority, COUNT(orderpriority) FROM orders GROUP BY orderpriority UNION " +
                        "SELECT NULL, COUNT(orderpriority) FROM orders");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesOnGroupedColumn()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesOnUngroupedColumn()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesWithGroupedColumns()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithSingleDistinctAndUnion()
            throws Exception
    {
        assertQuery("SELECT suppkey, COUNT(DISTINCT linenumber) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((suppkey), ())",
                "SELECT suppkey, COUNT(DISTINCT linenumber) FROM lineitem GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithSingleDistinctAndUnionGroupedArguments()
            throws Exception
    {
        assertQuery("SELECT linenumber, COUNT(DISTINCT linenumber) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((linenumber), ())",
                "SELECT DISTINCT linenumber, 1 FROM lineitem UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinctAndUnion()
            throws Exception
    {
        assertQuery("SELECT linenumber, COUNT(DISTINCT linenumber), SUM(DISTINCT suppkey) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((linenumber), ())",
                "SELECT linenumber, 1, SUM(DISTINCT suppkey) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber), SUM(DISTINCT suppkey) FROM lineitem");
    }

    @Test
    public void testRollup()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY ROLLUP (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testCube()
            throws Exception
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY CUBE (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingCombinationsAll()
            throws Exception
    {
        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, ROLLUP (suppkey, linenumber), CUBE (linenumber)",
                "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey UNION ALL " +
                        "SELECT orderkey, partkey, NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey");
    }

    @Test
    public void testGroupingCombinationsDistinct()
            throws Exception
    {
        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY DISTINCT orderkey, partkey, ROLLUP (suppkey, linenumber), CUBE (linenumber)",
                "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey UNION ALL " +
                        "SELECT orderkey, partkey, NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey");
    }

    @Test
    public void testRollupOverUnion()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderstatus, sum(orderkey)\n" +
                "FROM (SELECT orderkey, orderstatus\n" +
                "      FROM orders\n" +
                "      UNION ALL\n" +
                "      SELECT orderkey, orderstatus\n" +
                "      FROM orders) x\n" +
                "GROUP BY ROLLUP (orderstatus)",
                "VALUES ('P', 21470000),\n" +
                        "('O', 439774330),\n" +
                        "('F', 438500670),\n" +
                        "(NULL, 899745000)");
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        assertQuery("SELECT regionkey FROM nation WHERE nationkey < 7 INTERSECT select regionkey FROM nation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery("SELECT regionkey FROM nation WHERE nationkey < 7 INTERSECT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery("WITH wnation AS (SELECT nationkey, regionkey FROM nation) SELECT regionkey FROM wnation WHERE nationkey < 7 INTERSECT SELECT regionkey FROM wnation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery("SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 INTERSECT SELECT 1 FROM nation WHERE nationkey=20) T", "SELECT 1");
        assertQuery("SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 INTERSECT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0", "VALUES (6,3), (8,4)");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) UNION SELECT 4", "VALUES 3, 1, 4");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 UNION SELECT regionkey FROM nation WHERE nationkey > 21) INTERSECT SELECT 1", "SELECT 1");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) UNION ALL SELECT 3", "VALUES 3, 1, 3");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) UNION ALL SELECT 3", "VALUES 3, 1, 3");
        assertQuery("SELECT * FROM (VALUES 1, 2) INTERSECT SELECT * FROM (VALUES 1.0, 2)", "VALUES 1.0, 2.0");

        MaterializedResult emptyResult = computeActual("SELECT 100 INTERSECT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testIntersectWithAggregation()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM nation INTERSECT SELECT COUNT(regionkey) FROM nation HAVING SUM(regionkey) IS NOT NULL", "SELECT 25");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey,name FROM nation INTERSECT SELECT regionkey, name FROM nation) n", "VALUES (5, 3)");
        assertQuery("SELECT COUNT(*) * 2 FROM nation INTERSECT (SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2)", "SELECT 50");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation INTERSECT SELECT regionkey FROM nation) n1 INTERSECT SELECT regionkey FROM nation) n2", "SELECT 5");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation INTERSECT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey", "VALUES (1, 2, 0), (1, 2, 1), (1, 2, 4)");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation INTERSECT SELECT 2) n1 INTERSECT SELECT regionkey FROM nation", "SELECT 1");
    }

    @Test
    public void testIntersectAllFails()
    {
        assertQueryFails("SELECT * FROM (VALUES 1, 2, 3, 4) INTERSECT ALL SELECT * FROM (VALUES 3, 4)", "line 1:35: INTERSECT ALL not yet implemented");
    }

    @Test
    public void testExcept()
            throws Exception
    {
        assertQuery("SELECT regionkey FROM nation WHERE nationkey < 7 EXCEPT select regionkey FROM nation WHERE nationkey > 21", "VALUES 0, 4");
        assertQuery("SELECT regionkey FROM nation WHERE nationkey < 7 EXCEPT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21", "VALUES 0, 4");
        assertQuery("WITH wnation AS (SELECT nationkey, regionkey FROM nation) SELECT regionkey FROM wnation WHERE nationkey < 7 EXCEPT SELECT regionkey FROM wnation WHERE nationkey > 21", "VALUES 0, 4");
        assertQuery("SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 EXCEPT SELECT 2 FROM nation WHERE nationkey=20) T", "SELECT 1");
        assertQuery("SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 EXCEPT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0", "VALUES (0,0), (2, 1), (4, 2)");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) UNION SELECT 3", "VALUES 0, 4, 3");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 UNION SELECT regionkey FROM nation WHERE nationkey > 21) EXCEPT SELECT 1", "VALUES 0, 3, 4");
        assertQuery("SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) UNION ALL SELECT 4", "VALUES 4, 0, 4");
        assertQuery("SELECT * FROM (VALUES 1, 2) EXCEPT SELECT * FROM (VALUES 3.0, 2)", "VALUES 1.0");

        MaterializedResult emptyResult = computeActual("SELECT 0 EXCEPT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testExceptWithAggregation()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM nation EXCEPT SELECT COUNT(regionkey) FROM nation where regionkey < 3 HAVING SUM(regionkey) IS NOT NULL", "SELECT 25");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey, name FROM nation where nationkey < 6 EXCEPT SELECT regionkey, name FROM nation) n", "VALUES (10, 3)");
        assertQuery("(SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2) EXCEPT SELECT COUNT(*) * 2 FROM nation", "SELECT 47");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation EXCEPT SELECT regionkey FROM nation) n1 EXCEPT SELECT regionkey FROM nation) n2", "SELECT 20");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation EXCEPT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey HAVING regionkey < 3", "VALUES (4, 8, 0), (4, 8, 1), (5, 10, 2)");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation EXCEPT SELECT 10) n1 EXCEPT SELECT regionkey FROM nation", "SELECT 24");
    }

    @Test
    public void testExceptAllFails()
    {
        assertQueryFails("SELECT * FROM (VALUES 1, 2, 3, 4) EXCEPT ALL SELECT * FROM (VALUES 3, 4)", "line 1:35: EXCEPT ALL not yet implemented");
    }

    @Test
    public void testCountAllWithComparison()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testSelectWithComparison()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testCountWithNotPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount");
    }

    @Test
    public void testCountWithNullPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NULL");
    }

    @Test
    public void testCountWithIsNullPredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F' "
        );
    }

    @Test
    public void testCountWithIsNotNullPredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NOT NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus <> 'F' "
        );
    }

    @Test
    public void testCountWithNullIfPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus ");
    }

    @Test
    public void testCountWithCoalescePredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE COALESCE(NULLIF(orderstatus, 'F'), 'bar') = 'bar'",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'"
        );
    }

    @Test
    public void testCountWithAndPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05");
    }

    @Test
    public void testCountWithOrPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05");
    }

    @Test
    public void testCountWithInlineView()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem) x");
    }

    @Test
    public void testNestedCount()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey, COUNT(*) FROM lineitem GROUP BY orderkey) x");
    }

    @Test
    public void testAggregationWithProjection()
            throws Exception
    {
        assertQuery("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders");
    }

    @Test
    public void testAggregationWithProjection2()
            throws Exception
    {
        assertQuery("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders");
    }

    @Test
    public void testGroupByOnSupersetOfPartitioning()
            throws Exception
    {
        assertQuery("SELECT orderdate, c, count(*) FROM (SELECT orderdate, count(*) c FROM orders GROUP BY orderdate) GROUP BY orderdate, c");
    }

    @Test
    public void testInlineView()
            throws Exception
    {
        assertQuery("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM ORDERS) U");
    }

    @Test
    public void testAliasedInInlineView()
            throws Exception
    {
        assertQuery("SELECT x, y FROM (SELECT orderkey x, custkey y FROM ORDERS) U");
    }

    @Test
    public void testInlineViewWithProjections()
            throws Exception
    {
        assertQuery("SELECT x + 1, y FROM (SELECT orderkey * 10 x, custkey y FROM ORDERS) u");
    }

    @Test
    public void testGroupByWithoutAggregation()
            throws Exception
    {
        assertQuery("SELECT orderstatus FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testNestedGroupByWithSameKey()
            throws Exception
    {
        assertQuery("SELECT custkey, sum(t) FROM (SELECT custkey, count(*) t FROM orders GROUP BY custkey) GROUP BY custkey");
    }

    @Test
    public void testGroupByWithNulls()
            throws Exception
    {
        assertQuery("SELECT key, COUNT(*) FROM (" +
                "SELECT CASE " +
                "  WHEN orderkey % 3 = 0 THEN NULL " +
                "  WHEN orderkey % 5 = 0 THEN 0 " +
                "  ELSE orderkey " +
                "  END as key " +
                "FROM lineitem) " +
                "GROUP BY key");
    }

    @Test
    public void testHistogram()
            throws Exception
    {
        assertQuery("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines");
    }

    @Test
    public void testSimpleJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("" +
                "SELECT COUNT(*) FROM " +
                "(SELECT orderkey FROM lineitem WHERE orderkey < 1000) a " +
                "JOIN " +
                "(SELECT orderkey FROM orders WHERE orderkey < 2000) b " +
                "ON NOT (a.orderkey <= b.orderkey)");
    }

    @Test
    public void testJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = 2");
    }

    @Test
    public void testJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testJoinDoubleClauseWithLeftOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testJoinDoubleClauseWithRightOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testJoinWithAlias()
            throws Exception
    {
        assertQuery("SELECT * FROM (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) x");
    }

    @Test
    public void testJoinWithConstantExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND 123 = 123");
    }

    @Test
    public void testJoinWithConstantPredicatePushDown()
            throws Exception
    {
        assertQuery("" +
                "SELECT\n" +
                "  a.orderstatus\n" +
                "  , a.clerk\n" +
                "FROM (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") a\n" +
                "INNER JOIN (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") b\n" +
                "ON\n" +
                "  a.orderstatus = b.orderstatus\n" +
                "  and a.clerk = b.clerk\n" +
                "where a.orderstatus = 'F'\n");
    }

    @Test
    public void testJoinWithInferredFalseJoinClause()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM orders\n" +
                "JOIN lineitem\n" +
                "ON CAST(orders.orderkey AS VARCHAR) = CAST(lineitem.orderkey AS VARCHAR)\n" +
                "WHERE orders.orderkey = 1 AND lineitem.orderkey = 2\n");
    }

    @Test
    public void testJoinUsing()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem join orders using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testJoinCriteriaCoercion()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (VALUES (1.0, 2.0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)",
                "VALUES (1.0, 2.0, 1, 3)"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (CAST (1 AS SMALLINT), CAST(3 AS SMALLINT))) y (a, b) USING(a)",
                "VALUES (1, 2, 1, 3)"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1.0, 2.0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) ON x.a = y.a",
                "VALUES (1.0, 2.0, 1, 3)"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (SMALLINT '1', SMALLINT '3')) y (a, b) ON x.a = y.a",
                "VALUES (1, 2, 1, 3)"
        );
    }

    @Test
    public void testJoinWithReversedComparison()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = lineitem.orderkey");
    }

    @Test
    public void testJoinWithComplexExpressions()
            throws Exception
    {
        assertQuery("SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CAST(orders.orderkey AS BIGINT)");
    }

    @Test
    public void testJoinWithComplexExpressions2()
            throws Exception
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CASE WHEN orders.custkey = 1 and orders.orderstatus = 'F' THEN orders.orderkey ELSE NULL END");
    }

    @Test
    public void testJoinWithComplexExpressions3()
            throws Exception
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey + 1 = orders.orderkey + 1",
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey "
                // H2 takes a million years because it can't join efficiently on a non-indexed field/expression
        );
    }

    @Test
    public void testSelfJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders a JOIN orders b on a.orderkey = b.orderkey");
    }

    @Test
    public void testWildcardFromJoin()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testQualifiedWildcardFromJoin()
            throws Exception
    {
        assertQuery(
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testJoinAggregations()
            throws Exception
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
    }

    @Test
    public void testNonEqualityJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON NOT NOT lineitem.orderkey = orders.orderkey AND NOT NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT NOT NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity <= 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity != 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate > orders.orderdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderdate < lineitem.shipdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment LIKE lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.comment LIKE orders.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE '%forges%')");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE lineitem.comment)");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NULL");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10)");
        assertQuery(
                "SELECT COUNT(*) FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 2",
                "VALUES (0)");
    }

    @Test
    public void testNonEqualityLeftJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         LEFT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      o.custkey != 1000 WHERE o.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000.0 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > orders.totalprice WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (1, 1, NULL,  NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c < d",
                "VALUES (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c < d",
                "VALUES (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON 1 = 1",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (1, NULL), (2, 11), (2, 10)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");

        assertQuery(
                "SELECT * FROM (VALUES 1) t1(a) LEFT OUTER JOIN (VALUES (1,2,2), (1,2,3), (1, 2, NULL)) t2(x,y,z) ON a=x AND y = z",
                "VALUES (1, 1, 2, 2)");
    }

    @Test
    public void testLeftJoinWithEmptyInnerTable()
            throws Exception
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON 1 = 1");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > 1");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > b.totalprice");
    }

    @Test
    public void testRightJoinWithEmptyInnerTable()
            throws Exception
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM orders b RIGHT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON 1 = 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > b.totalprice");
    }

    @Test
    public void testNonEqualityRightJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         RIGHT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      l.quantity != 5 WHERE l.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5.0 WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > lineitem.suppkey WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity*1000 > orders.totalprice WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice > 1000 WHERE lineitem.orderkey IS NULL");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c = d",
                "VALUES (1, 2, 1, 1), (1, 1, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c < d",
                "VALUES (NULL, NULL, 1, 1), (1, 2, 1, 2), (1, 1, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c < d",
                "VALUES (NULL, NULL, 1, 1), (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON 1 = 1",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testJoinUsingSymbolsFromJustOneSideOfJoin() throws Exception
    {
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (10), (11), (11)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2), (2)");
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (11), (11)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (1), (2), (2)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2), (2)");
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (11), (11)");
    }

    @Test
    public void testJoinsWithTrueJoinCondition() throws Exception
    {
        // inner join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) JOIN (VALUES 10, 11) t2(b) ON TRUE",
                 "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // left join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) LEFT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) LEFT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) LEFT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "VALUES (0, NULL), (1, NULL)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) LEFT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // right join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) RIGHT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) RIGHT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) RIGHT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) RIGHT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // full join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) FULL JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) FULL JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) FULL JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "VALUES (0, NULL), (1, NULL)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) FULL JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
    }

    @Test
    public void testNonEqualityFullJoin()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL"
        );
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL"
        );
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10), (1, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11), (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testJoinOnMultipleFields()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testJoinUsingMultipleFields()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem JOIN (SELECT orderkey, orderdate shipdate FROM ORDERS) T USING (orderkey, shipdate)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate"
        );
    }

    @Test
    public void testColocatedJoinWithLocalUnion()
            throws Exception
    {
        assertQuery(
                "select count(*) from ((select * from orders) union all (select * from orders)) join orders using (orderkey)",
                "select 2 * count(*) from orders");
    }

    @Test
    public void testJoinWithNonJoinExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testJoinWithNullValues()
            throws Exception
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithMultipleInSubqueryClauses()
            throws Exception
    {
        String queryPrefix = "SELECT * " +
                "FROM " +
                "    (VALUES 1,2,3) t(x) " +
                "  JOIN " +
                "    (VALUES 1,2,3) t2(y) " +
                "  ON ";

        assertQuery(
                queryPrefix + "(x in (VALUES 1,2,3)) = (y in (VALUES 1,2,3)) AND (x in (VALUES 1,2)) = (y in (VALUES 1,2))",
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3)");
        assertQuery(
                queryPrefix + "(x in (VALUES 1,2)) = (y in (VALUES 1,2)) AND (x in (VALUES 1)) = (y in (VALUES 3))",
                "VALUES (2,2), (2,1)");
        assertQuery(
                queryPrefix + "(x in (VALUES 1,2)) = (y in (VALUES 1,2)) AND (x in (VALUES 1)) != (y in (VALUES 3))",
                "VALUES (1,2), (1,1), (3, 3)");
        assertQuery(
                queryPrefix + "(x in (VALUES 1)) = (y in (VALUES 1)) AND (x in (SELECT 2)) != (y in (SELECT 2))",
                "VALUES (2,3), (3, 2)");
    }

    @Test
    public void testJoinWithInSubqueryToBeExecutedAsPostJoinFilter()
            throws Exception
    {
        String queryPrefix = "SELECT * " +
                "FROM " +
                "    (VALUES 1,2,3) t(x) " +
                "  JOIN " +
                "    (VALUES 1,2,3) t2(y) " +
                "  ON ";

        assertQuery(
                queryPrefix + " (x+y in (VALUES 4, 5))",
                "VALUES (1,3), (2,2), (2,3), (3,1), (3,2)");
        assertQuery(
                queryPrefix + " (x+y in (VALUES 4, 5)) AND (x*y in (VALUES 4))",
                "VALUES (2,2)");
        assertQuery(
                queryPrefix + " (x+y in (VALUES 4, 5)) = (x*y IN (VALUES 4,5))",
                "VALUES (1,1), (1,2), (2,1), (2,2), (3,3)");
        assertQuery(
                queryPrefix + "(x+y in (VALUES (4),(5))) " +
                        "AND " +
                        "(x in (VALUES 1)) != (y in (VALUES 3))",
                "VALUES (2,3)");
        assertQuery(
                queryPrefix + "(x+y in (VALUES (4),(5))) " +
                        "AND " +
                        "(x in (VALUES 1)) = (y in (VALUES 3))",
                "VALUES (1,3), (2,2), (3,2), (3,1)");
    }

    @Test
    public void testJoinWithMultipleScalarSubqueryClauses()
            throws Exception
    {
        String queryPrefix = "SELECT * " +
                "FROM " +
                "    (VALUES 1,2,3) t(x) " +
                "  JOIN " +
                "    (VALUES 1,2,3) t2(y) " +
                "  ON ";

        assertQuery(
                queryPrefix + "(x = (VALUES 1)) = (y = (VALUES 2)) AND (x in (VALUES 2)) = (y in (VALUES 1))",
                "VALUES (1,2), (2,1), (3, 3)");
        assertQuery(
                queryPrefix + "(x = (VALUES 2)) = (y > (VALUES 0)) AND (x > (VALUES 1)) = (y < (VALUES 3))",
                "VALUES (2,2), (2,1)");
        assertQuery(
                queryPrefix + "(x = (VALUES 1)) = (y = (VALUES 1)) AND (x = (SELECT 2)) != (y = (SELECT 3))",
                "VALUES (2,2), (3,3)");
    }

    @Test
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilter()
            throws Exception
    {
        String queryPrefix = "SELECT * " +
                "FROM " +
                "    (VALUES 1,2,3) t(x) " +
                "  JOIN " +
                "    (VALUES 1,2,3) t2(y) " +
                "  ON ";

        assertQuery(
                queryPrefix + " (x+y = (SELECT 4))",
                "VALUES (1,3), (2,2), (3,1)");
        assertQuery(
                queryPrefix + "(x+y = (VALUES 4)) AND (x*y = (VALUES 4))",
                "VALUES (2,2)");
        assertQuery(
                queryPrefix + "(x+y > (VALUES 2)) = (x*y  > (VALUES 8))",
                "VALUES (1,1), (3,3)");
        assertQuery(
                queryPrefix + "x+y >= (VALUES 4) " +
                        "AND " +
                        "(x = (VALUES 2)) != (y = (VALUES 2))",
                "VALUES (3,2), (2,3)");
        assertQuery(
                queryPrefix + "(x+y >= (VALUES 4)) " +
                        "AND " +
                        "(x = (VALUES 3)) = (y = (VALUES 3))",
                "VALUES (2, 2), (3, 3)");
    }

    @Test
    public void testLeftFilteredJoin()
            throws Exception
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testRightFilteredJoin()
            throws Exception
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testJoinWithFullyPushedDownJoinClause()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.custkey = 1 AND lineitem.orderkey = 1");
    }

    @Test
    public void testJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0\n" +
                "WHERE orders.custkey % 8 < 7 AND orders.custkey % 8 = lineitem.orderkey % 8 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testSimpleFullJoin()
            throws Exception
    {
        assertQuery("SELECT a, b FROM (VALUES (1), (2)) t (a) FULL OUTER JOIN (VALUES (1), (3)) u (b) ON a = b",
                "SELECT * FROM (VALUES (1, 1), (2, NULL), (NULL, 3))");
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT COUNT(*) FROM (" +
                    "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                    "UNION ALL " +
                    "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                    "WHERE lineitem.orderkey IS NULL" +
                ")");
        assertQuery("SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT COUNT(*) FROM (" +
                    "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                    "UNION ALL " +
                    "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                    "WHERE lineitem.orderkey IS NULL" +
                ")");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.custkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testFullJoinNormalizedToLeft()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testFullJoinNormalizedToRight()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey  WHERE orders.orderkey IS NOT NULL");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey WHERE orders.custkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey  WHERE orders.custkey IS NOT NULL");
    }

    @Test
    public void testFullJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON lineitem.orderkey = 1024",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = 1024 " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = 1024 " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testFullJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON orders.orderkey = 1024",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT OUTER JOIN orders ON orders.orderkey = 1024 " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT OUTER JOIN orders ON orders.orderkey = 1024 " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testSimpleFullJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2" +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2" +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testSimpleFullJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2" +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2" +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testOuterJoinWithNullsOnProbe()
            throws Exception
    {
        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 10 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey");

        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "FULL OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey",
                "SELECT DISTINCT orderkey FROM (" +
                        "SELECT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey " +
                        "UNION ALL " +
                        "SELECT a.orderkey FROM" +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "LEFT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey " +
                        "WHERE a.orderkey IS NULL)");
    }

    @Test
    public void testOuterJoinWithCommonExpression()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT count(1), count(one) " +
                "FROM (values (1, 'a'), (2, 'a')) as l(k, a) " +
                "LEFT JOIN (select k, 1 one from (values 1) as r(k)) r " +
                "ON l.k = r.k GROUP BY a");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2L, 1L) // (total rows, # of non null values)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testSimpleLeftJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");

        // With null base (null row value) in dereference expression
        assertQuery(
                "SELECT x.val FROM " +
                        "(SELECT CAST(ROW(v) as ROW(val integer)) FROM (VALUES 1, 2, 3) t(v)) ta (x) " +
                        "LEFT OUTER JOIN " +
                        "(SELECT CAST(ROW(v) as ROW(val integer)) FROM (VALUES 1, 2, 3) t(v)) tb (y) " +
                        "ON x.val=y.val " +
                        "WHERE y.val=1",
                "SELECT 1");
    }

    @Test
    public void testLeftJoinNormalizedToInner()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL");
    }

    @Test
    public void testLeftJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testSimpleLeftJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleLeftJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinDoubleClauseWithLeftOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testLeftJoinDoubleClauseWithRightOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredLeftJoin()
            throws Exception
    {
        assertQuery("SELECT * FROM lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testProbeFilteredLeftJoin()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a LEFT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testLeftJoinEqualityInference()
            throws Exception
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testLeftJoinWithNullValues()
            throws Exception
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testSimpleRightJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");

        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinNormalizedToInner()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testRightJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testRightJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testSimpleRightJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleRightJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testRightJoinDoubleClauseWithLeftOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinDoubleClauseWithRightOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredRightJoin()
            throws Exception
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a RIGHT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testProbeFilteredRightJoin()
            throws Exception
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem RIGHT JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testRightJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testRightJoinEqualityInference()
            throws Exception
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testRightJoinWithNullValues()
            throws Exception
    {
        assertQuery("" +
                "SELECT lineitem.orderkey, orders.orderkey\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "RIGHT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithDuplicateRelations()
            throws Exception
    {
        assertQuery("SELECT * FROM orders JOIN orders USING (orderkey)", "SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey");
        assertQuery("SELECT * FROM lineitem x JOIN orders x USING (orderkey)", "SELECT * FROM lineitem l JOIN orders o ON l.orderkey = o.orderkey");
    }

    @Test
    public void testOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testOrderBy2()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderkey DESC");
    }

    @Test
    public void testOrderByMultipleFields()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY custkey DESC, orderstatus");
    }

    @Test
    public void testOrderByDuplicateFields()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, custkey FROM orders ORDER BY custkey, custkey");
        assertQueryOrdered("SELECT custkey, custkey FROM orders ORDER BY custkey ASC, custkey DESC");
    }

    @Test
    public void testOrderByWithNulls()
            throws Exception
    {
        // nulls first
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS FIRST, custkey ASC");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS FIRST, custkey ASC");

        // nulls last
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS LAST, custkey ASC");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC, custkey ASC",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC");
    }

    @Test
    public void testOrderByAlias()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus x FROM orders ORDER BY x ASC");
    }

    @Test
    public void testOrderByAliasWithSameNameAsUnselectedColumn()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus orderdate FROM orders ORDER BY orderdate ASC");
    }

    @Test
    public void testOrderByOrdinal()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus, orderdate FROM orders ORDER BY 2, 1");
    }

    @Test
    public void testOrderByOrdinalWithWildcard()
            throws Exception
    {
        assertQueryOrdered("SELECT * FROM orders ORDER BY 1");
    }

    @Test
    public void testGroupByOrdinal()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY 1",
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testGroupBySearchedCase()
            throws Exception
    {
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");
    }

    @Test
    public void testGroupBySearchedCaseNoElse()
            throws Exception
    {
        // whole CASE in group by clause
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        assertQuery("SELECT CASE WHEN true THEN orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCase()
            throws Exception
    {
        // whole CASE in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        // operand in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in group by clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in group by clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus ELSE 'x' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'else' in group by clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN 'x' ELSE orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCaseNoElse()
            throws Exception
    {
        // whole CASE in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' END");

        // operand in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in group by clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in group by clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCast()
            throws Exception
    {
        // whole CAST in group by expression
        assertQuery("SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY CAST(orderkey AS VARCHAR)");

        assertQuery(
                "SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY 1",
                "SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY CAST(orderkey AS VARCHAR)");

        // argument in group by expression
        assertQuery("SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByCoalesce()
            throws Exception
    {
        // whole COALESCE in group by
        assertQuery("SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)");

        assertQuery(
                "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY 1",
                "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)"
        );

        // operands in group by
        assertQuery("SELECT COALESCE(orderkey, 1), count(*) FROM orders GROUP BY orderkey");

        // operands in group by
        assertQuery("SELECT COALESCE(1, orderkey), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByNullIf()
            throws Exception
    {
        // whole NULLIF in group by
        assertQuery("SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY NULLIF(orderkey, custkey)");

        assertQuery(
                "SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY 1",
                "SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY NULLIF(orderkey, custkey)");

        // first operand in group by
        assertQuery("SELECT NULLIF(orderkey, 1), count(*) FROM orders GROUP BY orderkey");

        // second operand in group by
        assertQuery("SELECT NULLIF(1, orderkey), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByExtract()
            throws Exception
    {
        // whole expression in group by
        assertQuery("SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM now())");

        assertQuery(
                "SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY 1",
                "SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM now())");

        // argument in group by
        assertQuery("SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY now()");
    }

    @Test
    public void testGroupByNullConstant()
            throws Exception
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM (\n" +
                "  SELECT cast(null as VARCHAR) constant, orderdate\n" +
                "  FROM orders\n" +
                ") a\n" +
                "group by constant, orderdate\n");
    }

    @Test
    public void testChecksum()
            throws Exception
    {
        assertQuery("SELECT to_hex(checksum(0))", "select '0000000000000000'");
    }

    @Test
    public void testMaxBy()
            throws Exception
    {
        assertQuery("SELECT MAX_BY(orderkey, totalprice) FROM orders", "SELECT orderkey FROM orders ORDER BY totalprice DESC LIMIT 1");
    }

    @Test
    public void testMaxByN()
            throws Exception
    {
        assertQuery("SELECT y FROM (SELECT MAX_BY(orderkey, totalprice, 2) mx FROM orders) CROSS JOIN UNNEST(mx) u(y)",
                "SELECT orderkey FROM orders ORDER BY totalprice DESC LIMIT 2");
    }

    @Test
    public void testMinBy()
            throws Exception
    {
        assertQuery("SELECT MIN_BY(orderkey, totalprice) FROM orders", "SELECT orderkey FROM orders ORDER BY totalprice ASC LIMIT 1");
    }

    @Test
    public void testMinByN()
            throws Exception
    {
        assertQuery("SELECT y FROM (SELECT MIN_BY(orderkey, totalprice, 2) mx FROM orders) CROSS JOIN UNNEST(mx) u(y)",
                "SELECT orderkey FROM orders ORDER BY totalprice ASC LIMIT 2");
    }

    @Test
    public void testGroupByBetween()
            throws Exception
    {
        // whole expression in group by
        assertQuery("SELECT orderkey BETWEEN 1 AND 100 FROM orders GROUP BY orderkey BETWEEN 1 AND 100 ");

        // expression in group by
        assertQuery("SELECT CAST(orderkey BETWEEN 1 AND 100 AS BIGINT) FROM orders GROUP BY orderkey");

        // min in group by
        assertQuery("SELECT CAST(50 BETWEEN orderkey AND 100 AS BIGINT) FROM orders GROUP BY orderkey");

        // max in group by
        assertQuery("SELECT CAST(50 BETWEEN 1 AND orderkey AS BIGINT) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testAggregationImplicitCoercion()
            throws Exception
    {
        assertQuery("SELECT 1.0 / COUNT(*) FROM orders");
        assertQuery("SELECT custkey, 1.0 / COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testWindowImplicitCoercion()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey, 1.0 / row_number() OVER (ORDER BY orderkey) FROM orders LIMIT 2");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, DOUBLE)
                .row(1L, 1.0)
                .row(2L, 0.5)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testHaving()
            throws Exception
    {
        assertQuery("SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus HAVING orderstatus = 'O'");
    }

    @Test
    public void testHaving2()
            throws Exception
    {
        assertQuery("SELECT custkey, sum(orderkey) FROM orders GROUP BY custkey HAVING sum(orderkey) > 400000");
    }

    @Test
    public void testHaving3()
            throws Exception
    {
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey");
        assertQuery("SELECT custkey, avg(totalprice + 5) FROM orders GROUP BY custkey");
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey HAVING avg(totalprice + 5) > 10");
    }

    @Test
    public void testHavingWithoutGroupBy()
            throws Exception
    {
        assertQuery("SELECT sum(orderkey) FROM orders HAVING sum(orderkey) > 400000");
    }

    @Test
    public void testGroupByAsJoinProbe()
            throws Exception
    {
        // we join on customer key instead of order key because
        // orders is effectively distributed on order key due the
        // generated data being sorted
        assertQuery("SELECT " +
                "  b.orderkey, " +
                "  b.custkey, " +
                "  a.custkey " +
                "FROM ( " +
                "  SELECT custkey" +
                "  FROM orders " +
                "  GROUP BY custkey" +
                ") a " +
                "JOIN orders b " +
                "  ON a.custkey = b.custkey ");
    }

    @Test
    public void testJoinEffectivePredicateWithNoRanges()
            throws Exception
    {
        assertQuery("" +
                "SELECT * FROM orders a " +
                "   JOIN (SELECT * FROM orders WHERE orderkey IS NULL) b " +
                "   ON a.orderkey = b.orderkey");
    }

    @Test
    public void testColumnAliases()
            throws Exception
    {
        assertQuery(
                "SELECT x, T.y, z + 1 FROM (SELECT custkey, orderstatus, totalprice FROM orders) T (x, y, z)",
                "SELECT custkey, orderstatus, totalprice + 1 FROM orders");
    }

    @Test
    public void testSameInputToAggregates()
            throws Exception
    {
        assertQuery("SELECT max(a), max(b) FROM (SELECT custkey a, custkey b FROM orders) x");
    }

    @Test
    public void testWindowFunctionWithImplicitCoercion()
            throws Exception
    {
        assertQuery("SELECT *, 1.0 * sum(x) OVER () FROM (VALUES 1) t(x)", "SELECT 1, 1.0");
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    @Test
    public void testWindowFunctionsExpressions()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus\n" +
                ", row_number() OVER (ORDER BY orderkey * 2) *\n" +
                "  row_number() OVER (ORDER BY orderkey DESC) + 100\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L, "O", (1L * 10) + 100)
                .row(2L, "O", (2L * 9) + 100)
                .row(3L, "F", (3L * 8) + 100)
                .row(4L, "O", (4L * 7) + 100)
                .row(5L, "F", (5L * 6) + 100)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowFunctionsFromAggregate()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "  SELECT orderstatus, clerk, sales\n" +
                "  , rank() OVER (PARTITION BY x.orderstatus ORDER BY sales DESC) rnk\n" +
                "  FROM (\n" +
                "    SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                "    FROM orders\n" +
                "    GROUP BY orderstatus, clerk\n" +
                "   ) x\n" +
                ") x\n" +
                "WHERE rnk <= 2\n" +
                "ORDER BY orderstatus, rnk");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, DOUBLE, BIGINT)
                .row("F", "Clerk#000000090", 2784836.61, 1L)
                .row("F", "Clerk#000000084", 2674447.15, 2L)
                .row("O", "Clerk#000000500", 2569878.29, 1L)
                .row("O", "Clerk#000000050", 2500162.92, 2L)
                .row("P", "Clerk#000000071", 841820.99, 1L)
                .row("P", "Clerk#000001000", 643679.49, 2L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(34L, 10L)
                .row(33L, 9L)
                .row(32L, 8L)
                .row(7L, 7L)
                .row(6L, 6L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testRowNumberNoOptimization()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE NOT rn <= 10");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), all.getMaterializedRows().size() - 10);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn - 5 <= 10");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);
    }

    @Test
    public void testRowNumberLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderstatus\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);

        actual = computeActual("" +
                "SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);

        actual = computeActual("" +
                "SELECT row_number() OVER () rn, orderstatus\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);

        actual = computeActual("" +
                "SELECT row_number() OVER (ORDER BY orderkey) rn\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);
    }

    @Test
    public void testRowNumberMultipleFilters()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn < 3 AND rn % 2 = 0 AND a = 2 LIMIT 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testRowNumberFilterAndLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (" +
                "SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "FROM (VALUES (1), (2), (1), (2)) t (a)) t WHERE rn < 2 LIMIT 2");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1L)
                .row(2, 1L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (" +
                "SELECT a, row_number() OVER (PARTITION BY a) rn\n" +
                "FROM (VALUES (1), (2), (1), (2), (1)) t (a)) t WHERE rn < 3 LIMIT 2");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1L)
                .row(1, 2L)
                .row(2, 1L)
                .row(2, 2L)
                .build();
        assertEquals(actual.getMaterializedRows().size(), 2);
        assertContains(expected, actual);
    }

    @Test
    public void testRowNumberUnpartitionedFilter()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5 AND orderstatus != 'Z'");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 5);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn < 5");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 4);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") LIMIT 5");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 5);
        assertContains(all, actual);
    }

    @Test
    public void testRowNumberPartitionedFilter()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT orderkey FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);
    }

    @Test
    public void testRowNumberJoin()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT a, rn\n" +
                "FROM (\n" +
                "    SELECT a, row_number() OVER (ORDER BY a) rn\n" +
                "    FROM (VALUES (1), (2)) t (a)\n" +
                ") a\n" +
                "JOIN (VALUES (2)) b (b) ON a.a = b.b\n" +
                "LIMIT 1");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("SELECT a, rn\n" +
                "FROM (\n" +
                "    SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "    FROM (VALUES (1), (2), (1), (2)) t (a)\n" +
                ") a\n" +
                "JOIN (VALUES (2)) b (b) ON a.a = b.b\n" +
                "LIMIT 2");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 1L)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testRowNumberUnpartitionedFilterLimit()
            throws Exception
    {
        assertQuery("" +
                "SELECT row_number() OVER ()\n" +
                "FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey\n" +
                "WHERE orders.orderkey = 10000\n" +
                "LIMIT 20");
    }

    @Test
    public void testRowNumberPropertyDerivation()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus, SUM(rn) OVER (PARTITION BY orderstatus) c\n" +
                "FROM (\n" +
                "   SELECT orderkey, orderstatus, row_number() OVER (PARTITION BY orderstatus) rn\n" +
                "   FROM (\n" +
                "       SELECT * FROM orders ORDER BY orderkey LIMIT 10\n" +
                "   )\n" +
                ")");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L, "O", 21L)
                .row(2L, "O", 21L)
                .row(3L, "F", 10L)
                .row(4L, "O", 21L)
                .row(5L, "F", 10L)
                .row(6L, "F", 10L)
                .row(7L, "O", 21L)
                .row(32L, "O", 21L)
                .row(33L, "F", 10L)
                .row(34L, "O", 21L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowPropertyDerivation()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, orderkey,\n" +
                "SUM(s) OVER (PARTITION BY orderstatus),\n" +
                "SUM(s) OVER (PARTITION BY orderstatus, orderkey),\n" +
                "SUM(s) OVER (PARTITION BY orderstatus ORDER BY orderkey),\n" +
                "SUM(s) OVER (ORDER BY orderstatus, orderkey)\n" +
                "FROM (\n" +
                "   SELECT orderkey, orderstatus, SUM(orderkey) OVER (ORDER BY orderstatus, orderkey) s\n" +
                "   FROM (\n" +
                "       SELECT * FROM orders ORDER BY orderkey LIMIT 10\n" +
                "   )\n" +
                ")");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .row("F", 3L, 72L, 3L, 3L, 3L)
                .row("F", 5L, 72L, 8L, 11L, 11L)
                .row("F", 6L, 72L, 14L, 25L, 25L)
                .row("F", 33L, 72L, 47L, 72L, 72L)
                .row("O", 1L, 433L, 48L, 48L, 120L)
                .row("O", 2L, 433L, 50L, 98L, 170L)
                .row("O", 4L, 433L, 54L, 152L, 224L)
                .row("O", 7L, 433L, 61L, 213L, 285L)
                .row("O", 32L, 433L, 93L, 306L, 378L)
                .row("O", 34L, 433L, 127L, 433L, 505L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNUnpartitionedWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        String sql = "SELECT row_number() OVER (), orderkey, orderstatus FROM orders ORDER BY orderkey LIMIT 5";
        MaterializedResult expected = computeExpected(sql, actual.getTypes());
        assertEquals(actual, expected);
    }

    @Test
    public void testTopNUnpartitionedLargeWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 10000");
        String sql = "SELECT row_number() OVER (), orderkey, orderstatus FROM orders ORDER BY orderkey LIMIT 10000";
        MaterializedResult expected = computeExpected(sql, actual.getTypes());
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testTopNPartitionedWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(1L, 1L, "O")
                .row(2L, 2L, "O")
                .row(1L, 3L, "F")
                .row(2L, 5L, "F")
                .row(1L, 65L, "P")
                .row(2L, 197L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2");
        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(1L, 3L)
                .row(2L, 5L)
                .row(1L, 65L)
                .row(2L, 197L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2");
        expected = resultBuilder(getSession(), BIGINT, VARCHAR)
                .row(1L, "O")
                .row(2L, "O")
                .row(1L, "F")
                .row(2L, "F")
                .row(1L, "P")
                .row(2L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNUnpartitionedWindowWithEqualityFilter()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(2L, 2L, "O")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNUnpartitionedWindowWithCompositeFilter()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 1 OR rn IN (3, 4) OR rn BETWEEN 6 AND 7");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(1L, 1L, "O")
                .row(3L, 3L, "F")
                .row(4L, 4L, "O")
                .row(6L, 6L, "F")
                .row(7L, 7L, "O")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNPartitionedWindowWithEqualityFilter()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(2L, 2L, "O")
                .row(2L, 5L, "F")
                .row(2L, 197L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2L, 2L)
                .row(2L, 5L)
                .row(2L, 197L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        expected = resultBuilder(getSession(), BIGINT, VARCHAR)
                .row(2L, "O")
                .row(2L, "F")
                .row(2L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowFunctionWithGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT *, rank() OVER (PARTITION BY x)\n" +
                "FROM (SELECT 'foo' x)\n" +
                "GROUP BY 1");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, BIGINT)
                .row("foo", 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testPartialPrePartitionedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (PARTITION BY orderkey, custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 1L)
                .row(3L, 1L)
                .row(4L, 1L)
                .row(5L, 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullPrePartitionedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (PARTITION BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 1L)
                .row(3L, 1L)
                .row(4L, 1L)
                .row(5L, 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testPartialPreSortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (ORDER BY orderkey, custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullPreSortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (ORDER BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullyPartitionedAndPartiallySortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, custkey, orderPriority, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey, orderPriority)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey, custkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR, BIGINT)
                .row(1L, 370L, "5-LOW", 1L)
                .row(2L, 781L, "1-URGENT", 1L)
                .row(3L, 1234L, "5-LOW", 1L)
                .row(4L, 1369L, "5-LOW", 1L)
                .row(5L, 445L, "5-LOW", 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullyPartitionedAndFullySortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, custkey, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey, custkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR, BIGINT)
                .row(1L, 370L, 1L)
                .row(2L, 781L, 1L)
                .row(3L, 1234L, 1L)
                .row(4L, 1369L, 1L)
                .row(5L, 445L, 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByWindowFunctionWithNulls()
            throws Exception
    {
        MaterializedResult actual;
        MaterializedResult expected;

        // Nulls first
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3) NULLS FIRST)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 ASC\n" +
                "LIMIT 5");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(3L, 1L)
                .row(1L, 2L)
                .row(2L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .build();

        assertEquals(actual, expected);

        // Nulls last
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3) NULLS LAST)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(3L, 10L)
                .row(34L, 9L)
                .row(33L, 8L)
                .row(32L, 7L)
                .row(7L, 6L)
                .build();

        assertEquals(actual, expected);

        // and nulls last should be the default
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3))\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        assertEquals(actual, expected);
    }

    @Test
    public void testValueWindowFunctions()
    {
        MaterializedResult actual = computeActual("SELECT * FROM (\n" +
                "  SELECT orderkey, orderstatus\n" +
                "    , first_value(orderkey + 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey) fvalue\n" +
                "    , nth_value(orderkey + 1000, 2) OVER (PARTITION BY orderstatus ORDER BY orderkey\n" +
                "        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) nvalue\n" +
                "    FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "  ) x\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT, BIGINT)
                .row(1L, "O", 1001L, 1002L)
                .row(2L, "O", 1001L, 1002L)
                .row(3L, "F", 1003L, 1005L)
                .row(4L, "O", 1001L, 1002L)
                .row(5L, "F", 1003L, 1005L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowFrames()
    {
        MaterializedResult actual = computeActual("SELECT * FROM (\n" +
                "  SELECT orderkey, orderstatus\n" +
                "    , sum(orderkey + 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey\n" +
                "        ROWS BETWEEN mod(custkey, 2) PRECEDING AND custkey / 500 FOLLOWING)\n" +
                "    FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "  ) x\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L, "O", 1001L)
                .row(2L, "O", 3007L)
                .row(3L, "F", 3014L)
                .row(4L, "O", 4045L)
                .row(5L, "F", 2008L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowNoChannels()
    {
        MaterializedResult actual = computeActual("SELECT rank() OVER ()\n" +
                "FROM (SELECT * FROM orders LIMIT 10)\n" +
                "LIMIT 3");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L)
                .row(1L)
                .row(1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testScalarFunction()
            throws Exception
    {
        assertQuery("SELECT SUBSTR('Quadratically', 5, 6) FROM orders LIMIT 1");
    }

    @Test
    public void testCast()
            throws Exception
    {
        assertQuery("SELECT CAST('1' AS BIGINT) FROM orders");
        assertQuery("SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('1' AS BIGINT) FROM orders", "SELECT CAST('1' AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(totalprice AS BIGINT) FROM orders", "SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS DOUBLE) FROM orders", "SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS BOOLEAN) FROM orders", "SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('foo' AS BIGINT) FROM orders", "SELECT CAST(null AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(clerk AS BIGINT) FROM orders", "SELECT CAST(null AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey * orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey * orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(try_cast(orderkey AS VARCHAR) AS BIGINT) FROM orders", "SELECT orderkey FROM orders");
        assertQuery("SELECT try_cast(clerk AS VARCHAR) || try_cast(clerk AS VARCHAR) FROM orders", "SELECT clerk || clerk FROM orders");

        assertQuery("SELECT coalesce(try_cast('foo' AS BIGINT), 456) FROM orders", "SELECT 456 FROM orders");
        assertQuery("SELECT coalesce(try_cast(clerk AS BIGINT), 456) FROM orders", "SELECT 456 FROM orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "line 1:8: Cannot cast integer to date")
    public void testInvalidCast()
            throws Exception
    {
        assertQuery("SELECT CAST(1 AS DATE) FROM orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "line 2:1: Cannot cast integer to date")
    public void testInvalidCastInMultilineQuery()
            throws Exception
    {
        assertQuery("SELECT CAST(totalprice AS BIGINT),\n" +
                "CAST(2015 AS DATE),\n" +
                "CAST(orderkey AS DOUBLE) FROM orders");
    }

    @Test
    public void testTryInvalidCast()
            throws Exception
    {
        assertQuery("SELECT TRY(CAST('a' AS BIGINT))",
                "SELECT NULL");
    }

    @Test
    public void testConcatOperator()
            throws Exception
    {
        assertQuery("SELECT '12' || '34' FROM orders LIMIT 1");
    }

    @Test
    public void testQuotedIdentifiers()
            throws Exception
    {
        assertQuery("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "line 1:39: Column 'orderkey_1' cannot be resolved")
    public void testInvalidColumn()
            throws Exception
    {
        computeActual("select * from lineitem l join (select orderkey_1, custkey from orders) o on l.orderkey = o.orderkey_1");
    }

    @Test
    public void testUnaliasedSubqueries()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM (SELECT orderkey FROM orders)");
    }

    @Test
    public void testUnaliasedSubqueries1()
            throws Exception
    {
        assertQuery("SELECT a FROM (SELECT orderkey a FROM orders)");
    }

    @Test
    public void testJoinUnaliasedSubqueries()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM lineitem) join (SELECT * FROM orders) using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testWith()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT * FROM orders) " +
                "SELECT * FROM a",
                "SELECT * FROM orders");
    }

    @Test
    public void testWithQualifiedPrefix()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT 123 FROM orders LIMIT 1)" +
                "SELECT a.* FROM a",
                "SELECT 123 FROM orders LIMIT 1");
    }

    @Test
    public void testWithAliased()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT * FROM orders) " +
                "SELECT * FROM a x",
                "SELECT * FROM orders");
    }

    @Test
    public void testReferenceToWithQueryInFromClause()
            throws Exception
    {
        assertQuery(
                "WITH a AS (SELECT * FROM orders)" +
                        "SELECT * FROM (" +
                        "   SELECT * FROM a" +
                        ")",
                "SELECT * FROM orders");
    }

    @Test
    public void testWithChaining()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT orderkey n FROM orders)\n" +
                ", b AS (SELECT n + 1 n FROM a)\n" +
                ", c AS (SELECT n + 1 n FROM b)\n" +
                "SELECT n + 1 FROM c",
                "SELECT orderkey + 3 FROM orders");
    }

    @Test
    public void testWithSelfJoin()
            throws Exception
    {
        assertQuery("" +
                "WITH x AS (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "SELECT count(*) FROM x a JOIN x b USING (orderkey)", "" +
                "SELECT count(*)\n" +
                "FROM (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10) a\n" +
                "JOIN (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10) b ON a.orderkey = b.orderkey");
    }

    @Test
    public void testWithNestedSubqueries()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (\n" +
                "  WITH aa AS (SELECT 123 x FROM orders LIMIT 1)\n" +
                "  SELECT x y FROM aa\n" +
                "), b AS (\n" +
                "  WITH bb AS (\n" +
                "    WITH bbb AS (SELECT y FROM a)\n" +
                "    SELECT bbb.* FROM bbb\n" +
                "  )\n" +
                "  SELECT y z FROM bb\n" +
                ")\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "  WITH q AS (SELECT z w FROM b)\n" +
                "  SELECT j.*, k.*\n" +
                "  FROM a j\n" +
                "  JOIN q k ON (j.y = k.w)\n" +
                ") t", "" +
                "SELECT 123, 123 FROM orders LIMIT 1");
    }

    @Test
    public void testWithColumnAliasing()
            throws Exception
    {
        assertQuery(
                "WITH a (id) AS (SELECT 123 FROM orders LIMIT 1) SELECT id FROM a",
                "SELECT 123 FROM orders LIMIT 1");

        assertQuery(
                "WITH t (a, b, c) AS (SELECT 1, custkey x, orderkey FROM orders) SELECT c, b, a FROM t",
                "SELECT orderkey, custkey, 1 FROM orders");
    }

    @Test
    public void testWithHiding()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT 1), " +
                "     b AS (" +
                "         WITH a AS (SELECT 2)" +
                "         SELECT * FROM a" +
                "    )" +
                "SELECT * FROM b",
                "SELECT 2"
        );
        assertQueryFails("" +
                        "WITH a AS (VALUES 1), " +
                        "     a AS (VALUES 2)" +
                        "SELECT * FROM a",
                "line 1:28: WITH query name 'a' specified more than once"
        );
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "line 1:1: Recursive WITH queries are not supported")
    public void testWithRecursive()
            throws Exception
    {
        computeActual("WITH RECURSIVE a AS (SELECT 123) SELECT * FROM a");
    }

    @Test
    public void testCaseNoElse()
            throws Exception
    {
        assertQuery("SELECT orderkey, CASE orderstatus WHEN 'O' THEN 'a' END FROM orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:67: All CASE results must be the same type: varchar(1)\\E")
    public void testCaseNoElseInconsistentResultType()
        throws Exception
    {
        computeActual("SELECT orderkey, CASE orderstatus WHEN 'O' THEN 'a' WHEN '1' THEN 2 END FROM orders");
    }

    @Test
    public void testIfExpression()
            throws Exception
    {
        assertQuery(
                "SELECT sum(IF(orderstatus = 'F', totalprice, 0.0)) FROM orders",
                "SELECT sum(CASE WHEN orderstatus = 'F' THEN totalprice ELSE 0.0 END) FROM orders");
        assertQuery(
                "SELECT sum(IF(orderstatus = 'Z', totalprice)) FROM orders",
                "SELECT sum(CASE WHEN orderstatus = 'Z' THEN totalprice END) FROM orders");
        assertQuery(
                "SELECT sum(IF(orderstatus = 'F', NULL, totalprice)) FROM orders",
                "SELECT sum(CASE WHEN orderstatus = 'F' THEN NULL ELSE totalprice END) FROM orders");
        assertQuery(
                "SELECT IF(orderstatus = 'Z', orderkey / 0, orderkey) FROM orders",
                "SELECT CASE WHEN orderstatus = 'Z' THEN orderkey / 0 ELSE orderkey END FROM orders");
        assertQuery(
                "SELECT sum(IF(NULLIF(orderstatus, 'F') <> 'F', totalprice, 5.1)) FROM orders",
                "SELECT sum(CASE WHEN NULLIF(orderstatus, 'F') <> 'F' THEN totalprice ELSE 5.1 END) FROM orders");
    }

    @Test
    public void testIn()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0"); // H2 incorrectly matches rows
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2.0, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE totalprice IN (1, 2, 3)");
        assertQuery("SELECT x FROM (values 3, 100) t(x) WHERE x IN (2147483649)", "SELECT * WHERE false");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (2147483648, 2147483650)", "values 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (3, 4, 2147483648, 2147483650)", "values 3, 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 1, 2, 3) t(x) WHERE x IN (1 + cast(rand() < 0 as bigint), 2 + cast(rand() < 0 as bigint))", "values 1, 2");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (1 + cast(rand() < 0 as bigint), 2 + cast(rand() < 0 as bigint), 4)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (4, 2, 1)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 2147483648) t(x) WHERE x IN (1 + cast(rand() < 0 as bigint), 2 + cast(rand() < 0 as bigint), 2147483648)", "values 1, 2, 2147483648");
        assertQuery("SELECT x IN (0) FROM (values 4294967296) t(x)", "values false");
        assertQuery("SELECT x IN (0, 4294967297 + cast(rand() < 0 as bigint)) FROM (values 4294967296, 4294967297) t(x)", "values false, true");
        assertQuery("SELECT NULL in (1, 2, 3)", "values null");
        assertQuery("SELECT 1 in (1, NULL, 3)", "values true");
        assertQuery("SELECT 2 in (1, NULL, 3)", "values null");
        assertQuery("SELECT x FROM (values DATE '1970-01-01', DATE '1970-01-03') t(x) WHERE x IN (DATE '1970-01-01')", "values DATE '1970-01-01'");
        assertQuery("SELECT x FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1970-01-01 00:01:00+08:00') t(x) WHERE x IN (TIMESTAMP '1970-01-01 00:01:00+00:00')", "values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00'");
        assertQuery("SELECT COUNT(*) FROM (values 1) t(x) WHERE x IN (null, 0)", "SELECT 0");
    }

    @Test
    public void testLargeIn()
        throws Exception
    {
        String longValues =  range(0, 5000).asLongStream()
                .mapToObj(Long::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        String arrayValues = range(0, 5000).asLongStream()
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    public void testInSubqueryWithCrossJoin()
            throws Exception
    {
        assertQuery("SELECT a FROM (VALUES (1),(2)) t(a) WHERE a IN " +
                    "(SELECT b FROM (VALUES (ARRAY[2])) AS t1 (a) CROSS JOIN UNNEST(a) as t2(b))", "SELECT 2");
    }

    @Test
    public void testGroupByIf()
            throws Exception
    {
        assertQuery(
                "SELECT IF(orderkey between 1 and 5, 'orders', 'others'), sum(totalprice) FROM orders GROUP BY 1",
                "SELECT CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END, sum(totalprice)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END");
    }

    @Test
    public void testDuplicateFields()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (SELECT orderkey, orderkey FROM orders)",
                "SELECT orderkey, orderkey FROM orders");
    }

    @Test
    public void testDuplicateColumnsInWindowOrderByClause()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT a, row_number() OVER (ORDER BY a, a) FROM (VALUES 3, 2, 1) t(a)");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1L)
                .row(2, 2L)
                .row(3, 3L)
                .build();

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testWildcardFromSubquery()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT orderkey X FROM orders)");
    }

    @Test
    public void testCaseInsensitiveOutputAliasInOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT orderkey X FROM orders ORDER BY x");
    }

    @Test
    public void testCaseInsensitiveAttribute()
            throws Exception
    {
        assertQuery("SELECT x FROM (SELECT orderkey X FROM orders)");
    }

    @Test
    public void testCaseInsensitiveAliasedRelation()
            throws Exception
    {
        assertQuery("SELECT A.* FROM orders a");
    }

    @Test
    public void testCaseInsensitiveRowFieldReference()
            throws Exception
    {
        assertQuery("SELECT a.Col0 FROM (VALUES row(cast(ROW(1,2) as ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1");
    }

    @Test
    public void testSubqueryBody()
            throws Exception
    {
        assertQuery("(SELECT orderkey, custkey FROM ORDERS)");
    }

    @Test
    public void testSubqueryBodyOrderLimit()
            throws Exception
    {
        assertQueryOrdered("(SELECT orderkey AS a, custkey AS b FROM ORDERS) ORDER BY a LIMIT 1");
    }

    @Test
    public void testSubqueryBodyProjectedOrderby()
            throws Exception
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM ORDERS) ORDER BY orderkey * -1");
    }

    @Test
    public void testSubqueryBodyDoubleOrderby()
            throws Exception
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM ORDERS ORDER BY custkey) ORDER BY orderkey");
    }

    @Test
    public void testNodeRoster()
            throws Exception
    {
        List<MaterializedRow> result = computeActual("SELECT * FROM system.runtime.nodes").getMaterializedRows();
        assertEquals(result.size(), getNodeCount());
    }

    @Test
    public void testCountOnInternalTables()
            throws Exception
    {
        List<MaterializedRow> rows = computeActual("SELECT count(*) FROM system.runtime.nodes").getMaterializedRows();
        assertEquals(((Long) rows.get(0).getField(0)).longValue(), getNodeCount());
    }

    @Test
    public void testTransactionsTable()
            throws Exception
    {
        List<MaterializedRow> result = computeActual("SELECT * FROM system.runtime.transactions").getMaterializedRows();
        assertTrue(result.size() >= 1); // At least one row for the current transaction.
    }

    @Test
    public void testDefaultExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDefaultExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDistributedExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testExplainOfExplain()
    {
        String query = "EXPLAIN SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testExplainOfExplainAnalyze()
    {
        String query = "EXPLAIN ANALYZE SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testExplainDdl()
    {
        assertExplainDdl("CREATE TABLE foo (pk bigint)", "CREATE TABLE foo");
        assertExplainDdl("CREATE VIEW foo AS SELECT * FROM orders", "CREATE VIEW foo");
        assertExplainDdl("DROP TABLE orders");
        assertExplainDdl("DROP VIEW view");
        assertExplainDdl("ALTER TABLE orders RENAME TO new_name");
        assertExplainDdl("ALTER TABLE orders RENAME COLUMN orderkey TO new_column_name");
        assertExplainDdl("SET SESSION foo = 'bar'");
        assertExplainDdl("PREPARE my_query FROM SELECT * FROM orders", "PREPARE my_query");
        assertExplainDdl("DEALLOCATE PREPARE my_query");
        assertExplainDdl("RESET SESSION foo");
        assertExplainDdl("START TRANSACTION");
        assertExplainDdl("COMMIT");
        assertExplainDdl("ROLLBACK");
    }

    private void assertExplainDdl(String query)
    {
        assertExplainDdl(query, query);
    }

    private void assertExplainDdl(String query, String expected)
    {
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), expected);
    }

    @Test
    public void testExplainExecute()
    {
        Session session = getSession().withPreparedStatement("my_query", "SELECT * FROM orders");
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("SELECT * FROM orders", LOGICAL));
    }

    @Test
    public void testShowCatalogs()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW CATALOGS");
        assertTrue(result.getOnlyColumnAsSet().contains(getSession().getCatalog().get()));
    }

    @Test
    public void testShowCatalogsLike()
            throws Exception
    {
        MaterializedResult result = computeActual(format("SHOW CATALOGS LIKE '%s'", getSession().getCatalog().get()));
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getCatalog().get()));
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasFrom()
            throws Exception
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS FROM %s", getSession().getCatalog().get()));
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasLike()
            throws Exception
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS LIKE '%s'", getSession().getSchema().get()));
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getSchema().get()));
    }

    public void testShowTables()
            throws Exception
    {
        Set<String> expectedTables = ImmutableSet.copyOf(transform(TpchTable.getTables(), tableNameGetter()));

        MaterializedResult result = computeActual("SHOW TABLES");
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));
    }

    @Test
    public void testShowTablesFrom()
            throws Exception
    {
        Set<String> expectedTables = ImmutableSet.copyOf(transform(TpchTable.getTables(), tableNameGetter()));

        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();

        MaterializedResult result = computeActual("SHOW TABLES FROM " + schema);
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));

        result = computeActual("SHOW TABLES FROM " + catalog + "." + schema);
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));

        try {
            computeActual("SHOW TABLES FROM UNKNOWN");
            fail("Showing tables in an unknown schema should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), MISSING_SCHEMA);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:1: Schema 'unknown' does not exist");
        }
    }

    @Test
    public void testShowTablesLike()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW TABLES LIKE 'or%'");
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(ORDERS.getTableName()));
    }

    @Test
    public void testShowColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "")
                .row("custkey", "bigint", "")
                .row("orderstatus", "varchar", "")
                .row("totalprice", "double",  "")
                .row("orderdate", "date", "")
                .row("orderpriority", "varchar", "")
                .row("clerk", "varchar", "")
                .row("shippriority", "integer", "")
                .row("comment", "varchar", "")
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "")
                .row("custkey", "bigint", "")
                .row("orderstatus", "varchar(1)", "")
                .row("totalprice", "double",  "")
                .row("orderdate", "date", "")
                .row("orderpriority", "varchar(15)", "")
                .row("clerk", "varchar(15)", "")
                .row("shippriority", "integer", "")
                .row("comment", "varchar(79)", "")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertTrue(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar),
                format("%s does not matche neither of %s and %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar));
    }

    @Test
    public void testAtTimeZone()
            throws Exception
    {
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE INTERVAL '07:09' hour to minute", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT MIN(x) AT TIME ZONE 'America/Chicago' from (VALUES TIMESTAMP '1970-01-01 00:01:00+00:00') t(x)", "values TIMESTAMP '1970-01-01 00:01:00+00:00'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE '+07:09'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                "values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00'");
        assertQuery("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00', TIMESTAMP '1970-01-01 08:01:00', TIMESTAMP '1969-12-31 16:01:00') t(x)",
                "values TIMESTAMP '1969-12-31 16:01:00-08:00', TIMESTAMP '1970-01-01 00:01:00-08:00', TIMESTAMP '1969-12-31 08:01:00-08:00'");
        assertQuery("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                "values TIMESTAMP '1969-12-31 16:01:00-08:00'");

        // with chained AT TIME ZONE
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Tokyo' AT TIME ZONE 'America/Los_Angeles'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'Asia/Shanghai'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                "values TIMESTAMP '1969-12-31 16:01:00-08:00'");
    }

    @Test
    public void testShowFunctions()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        ImmutableMultimap<String, MaterializedRow> functions = Multimaps.index(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 6);
            return (String) input.getField(0);
        });

        assertTrue(functions.containsKey("avg"), "Expected function names " + functions + " to contain 'avg'");
        assertEquals(functions.get("avg").asList().size(), 3);
        assertEquals(functions.get("avg").asList().get(0).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(0).getField(2), "bigint");
        assertEquals(functions.get("avg").asList().get(0).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(1).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(1).getField(2), "double");
        assertEquals(functions.get("avg").asList().get(1).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(2).getField(1), "real");
        assertEquals(functions.get("avg").asList().get(2).getField(2), "real");
        assertEquals(functions.get("avg").asList().get(2).getField(3), "aggregate");

        assertTrue(functions.containsKey("abs"), "Expected function names " + functions + " to contain 'abs'");
        assertEquals(functions.get("abs").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("abs").asList().get(0).getField(4), true);

        assertTrue(functions.containsKey("rand"), "Expected function names " + functions + " to contain 'rand'");
        assertEquals(functions.get("rand").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("rand").asList().get(0).getField(4), false);

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'rank'");
        assertEquals(functions.get("rank").asList().get(0).getField(3), "window");

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'split_part'");
        assertEquals(functions.get("split_part").asList().get(0).getField(1), "varchar(x)");
        assertEquals(functions.get("split_part").asList().get(0).getField(2), "varchar(x), varchar, bigint");
        assertEquals(functions.get("split_part").asList().get(0).getField(3), "scalar");

        assertFalse(functions.containsKey("like"), "Expected function names " + functions + " not to contain 'like'");
    }

    @Test
    public void testInformationSchemaFiltering()
            throws Exception
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
    }

    @Test
    public void testSelectColumnOfNulls()
            throws Exception
    {
        // Currently nulls can confuse the local planner, so select some
        assertQueryOrdered("SELECT \n" +
                " CAST(NULL AS VARCHAR),\n" +
                " CAST(NULL AS BIGINT)\n" +
                "FROM ORDERS\n" +
                " ORDER BY 1");
    }

    @Test
    public void testSelectCaseInsensitive()
            throws Exception
    {
        assertQuery("SELECT ORDERKEY FROM ORDERS");
        assertQuery("SELECT OrDeRkEy FROM OrDeRs");
    }

    @Test
    public void testShowSession()
            throws Exception
    {
        MaterializedResult result = computeActual(
                getSession()
                        .withSystemProperty("test_string", "foo string")
                        .withSystemProperty("test_long", "424242")
                        .withCatalogProperty("connector", "connector_string", "bar string")
                        .withCatalogProperty("connector", "connector_long", "11"),
                "SHOW SESSION");

        ImmutableMap<String, MaterializedRow> properties = Maps.uniqueIndex(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 5);
            return (String) input.getField(0);
        });

        assertEquals(properties.get("test_string"), new MaterializedRow(1, "test_string", "foo string", "test default", "varchar", "test string property"));
        assertEquals(properties.get("test_long"), new MaterializedRow(1, "test_long", "424242", "42", "bigint", "test long property"));
        assertEquals(properties.get("connector.connector_string"), new MaterializedRow(1, "connector.connector_string", "bar string", "connector default", "varchar", "connector string property"));
        assertEquals(properties.get("connector.connector_long"), new MaterializedRow(1, "connector.connector_long", "11", "33", "bigint", "connector long property"));
    }

    @Test
    public void testTry()
            throws Exception
    {
        // divide by zero
        assertQuery(
                "SELECT linenumber, sum(TRY(100/(CAST (tax*10 AS BIGINT)))) FROM lineitem GROUP BY linenumber",
                "SELECT linenumber, sum(100/(CAST (tax*10 AS BIGINT))) FROM lineitem WHERE CAST(tax*10 AS BIGINT) <> 0 GROUP BY linenumber");

        // invalid cast
        assertQuery(
                "SELECT TRY(CAST(IF(round(totalprice) % 2 = 0, CAST(totalprice AS VARCHAR), '^&$' || CAST(totalprice AS VARCHAR)) AS DOUBLE)) FROM orders",
                "SELECT CASE WHEN round(totalprice) % 2 = 0 THEN totalprice ELSE null END FROM orders");

        // invalid function argument
        assertQuery(
                "SELECT COUNT(TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // as part of a complex expression
        assertQuery(
                "SELECT COUNT(CAST(orderkey AS VARCHAR) || TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");
    }

    // no regexp specified because the JVM optimizes away exception message constructor if run enough times
    @Test(expectedExceptions = RuntimeException.class)
    public void testTryNoMergeProjections()
        throws Exception
    {
        computeActual(
                "SELECT TRY(x) FROM (SELECT 1/y as x FROM (VALUES 1, 2, 3, 0, 4) t(y))");
    }

    @Test
    public void testNoFrom()
            throws Exception
    {
        assertQuery("SELECT 1 + 2, 3 + 4", "SELECT 1 + 2, 3 + 4 FROM orders LIMIT 1");
    }

    @Test
    public void testTopNByMultipleFields()
            throws Exception
    {
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey ASC, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey ASC, custkey DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey DESC, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey DESC, custkey DESC LIMIT 10");

        // now try with order by fields swapped
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey ASC, orderkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey ASC, orderkey DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey DESC, orderkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey DESC, orderkey DESC LIMIT 10");

        // nulls first
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS FIRST, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS FIRST, custkey ASC LIMIT 10");

        // nulls last
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS LAST, custkey ASC LIMIT 10");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC, custkey ASC LIMIT 10",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC LIMIT 10");
    }

    @Test
    public void testExchangeWithProjectionPushDown()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM \n" +
                "  (SELECT orderkey + 1 orderkey FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 100)) o \n" +
                "JOIN \n" +
                "  (SELECT orderkey + 1 orderkey FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 100)) o1 \n" +
                "ON (o.orderkey = o1.orderkey)");
    }

    @Test
    public void testUnionWithProjectionPushDown()
            throws Exception
    {
        assertQuery("SELECT key + 5, status FROM (SELECT orderkey key, orderstatus status FROM orders UNION ALL SELECT orderkey key, linestatus status FROM lineitem)");
    }

    @Test
    public void testJoinProjectionPushDown()
            throws Exception
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) t\n" +
                "JOIN\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) u\n" +
                "ON\n" +
                "  t.orderkey = u.orderkey");
    }

    @Test
    public void testUnion()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION SELECT custkey FROM orders");
        assertQuery("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123");
        assertQuery("SELECT NULL UNION SELECT NULL");

        // mixed single-node vs fixed vs source-distributed
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT 123 UNION ALL (SELECT custkey FROM orders GROUP BY custkey)");
    }

    @Test
    public void testUnionDistinct()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION DISTINCT SELECT custkey FROM orders");
    }

    @Test
    public void testUnionAll()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT custkey FROM orders");
    }

    @Test
    public void testUnionArray()
            throws Exception
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[1] UNION ALL SELECT ARRAY[1]) t(a) LIMIT 1", "SELECT 1");
    }

    @Test
    public void testChainedUnionsWithOrder()
            throws Exception
    {
        assertQueryOrdered(
                "SELECT orderkey FROM orders UNION (SELECT custkey FROM orders UNION SELECT linenumber FROM lineitem) UNION ALL SELECT orderkey FROM lineitem ORDER BY orderkey");
    }

    @Test
    public void testUnionWithJoin()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "JOIN orders o ON (a.orderkey = o.orderkey)");
    }

    @Test
    public void testUnionWithAggregation()
            throws Exception
    {
        assertQuery(
                "SELECT ds, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
    }

    @Test
    public void testUnionWithAggregationAndJoin()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "SELECT orderkey, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY orderkey) t " +
                        "JOIN orders o " +
                        "ON (o.orderkey = t.orderkey)");
    }

    @Test
    public void testUnionWithJoinOnNonTranslateableSymbols()
            throws Exception
    {
        assertQuery("SELECT *\n" +
                "FROM (SELECT orderdate ds, orderkey\n" +
                "      FROM orders\n" +
                "      UNION ALL\n" +
                "      SELECT shipdate ds, orderkey\n" +
                "      FROM lineitem) a\n" +
                "JOIN orders o\n" +
                "ON (substr(cast(a.ds AS VARCHAR), 6, 2) = substr(cast(o.orderdate AS VARCHAR), 6, 2) AND a.orderkey = o.orderkey)");
    }

    @Test
    public void testSubqueryUnion()
            throws Exception
    {
        assertQueryOrdered("SELECT * FROM (SELECT orderkey FROM orders UNION SELECT custkey FROM orders UNION SELECT orderkey FROM orders) ORDER BY orderkey LIMIT 1000");
    }

    @Test
    public void testUnionWithFilterNotInSelect()
            throws Exception
    {
        assertQuery("SELECT orderkey, orderdate FROM orders WHERE custkey < 1000 UNION ALL SELECT orderkey, shipdate FROM lineitem WHERE linenumber < 2000");
        assertQuery("SELECT orderkey, orderdate FROM orders UNION ALL SELECT orderkey, shipdate FROM lineitem WHERE linenumber < 2000");
        assertQuery("SELECT orderkey, orderdate FROM orders WHERE custkey < 1000 UNION ALL SELECT orderkey, shipdate FROM lineitem");
    }

    @Test
    public void testSelectOnlyUnion()
            throws Exception
    {
        assertQuery("SELECT 123, 'foo' UNION ALL SELECT 999, 'bar'");
    }

    @Test
    public void testMultiColumnUnionAll()
            throws Exception
    {
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testUnionRequiringCoercion()
            throws Exception
    {
        assertQuery("VALUES 1 UNION ALL VALUES 1.0, 2", "SELECT * FROM (VALUES 1) UNION ALL SELECT * FROM (VALUES 1.0, 2)");
        assertQuery("(VALUES 1) UNION ALL (VALUES 1.0, 2)", "SELECT * FROM (VALUES 1) UNION ALL SELECT * FROM (VALUES 1.0, 2)");
        assertQuery("SELECT 0, 0 UNION ALL SELECT 1.0, 0"); // This test case generates a RelationPlan whose .outputSymbols is different .root.outputSymbols
        assertQuery("SELECT 0, 0, 0, 0 UNION ALL SELECT 0.0, 0.0, 0, 0"); // This test case generates a RelationPlan where multiple positions share the same symbol
        assertQuery("SELECT * FROM (VALUES 1) UNION ALL SELECT * FROM (VALUES 1.0, 2)");

        assertQuery("SELECT * FROM (VALUES 1) UNION SELECT * FROM (VALUES 1.0, 2)", "VALUES 1.0, 2.0"); // H2 produces incorrect result for the original query: 1.0 1.0 2.0
        assertQuery("SELECT * FROM (VALUES (2, 2)) UNION SELECT * FROM (VALUES (1, 1.0))");
        assertQuery("SELECT * FROM (VALUES (NULL, NULL)) UNION SELECT * FROM (VALUES (1, 1.0))");
        assertQuery("SELECT * FROM (VALUES (NULL, NULL)) UNION ALL SELECT * FROM (VALUES (NULL, 1.0))");
    }

    @Test
    public void testTableQuery()
            throws Exception
    {
        assertQuery("TABLE orders", "SELECT * FROM orders");
    }

    @Test
    public void testTableQueryOrderLimit()
            throws Exception
    {
        assertQueryOrdered("TABLE orders ORDER BY orderkey LIMIT 10", "SELECT * FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Test
    public void testTableQueryInUnion()
            throws Exception
    {
        assertQuery("(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL TABLE orders", "(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testTableAsSubquery()
            throws Exception
    {
        assertQueryOrdered("(TABLE orders) ORDER BY orderkey", "(SELECT * FROM orders) ORDER BY orderkey");
    }

    @Test
    public void testLimitPushDown()
            throws Exception
    {
        MaterializedResult actual = computeActual(
                "(TABLE orders ORDER BY orderkey) UNION ALL " +
                        "SELECT * FROM orders WHERE orderstatus = 'F' UNION ALL " +
                        "(TABLE orders ORDER BY orderkey LIMIT 20) UNION ALL " +
                        "(TABLE orders LIMIT 5) UNION ALL " +
                        "TABLE orders LIMIT 10");
        MaterializedResult all = computeExpected("SELECT * FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testOrderLimitCompaction()
            throws Exception
    {
        assertQueryOrdered("SELECT * FROM (SELECT * FROM orders ORDER BY orderkey) LIMIT 10");
    }

    @Test
    public void testUnaliasSymbolReferencesWithUnion()
            throws Exception
    {
        assertQuery("SELECT 1, 1, 'a', 'a' UNION ALL SELECT 1, 2, 'a', 'b'");
    }

    @Test
    public void testRandCrossJoins()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY rand() LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY rand() LIMIT 5) b");
    }

    @Test
    public void testCrossJoins()
            throws Exception
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 5) b");
    }

    @Test
    public void testCrossJoinEmptyProbePage()
            throws Exception
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 0) a " +
                "CROSS JOIN (SELECT * FROM lineitem WHERE orderkey < 100) b");
    }

    @Test
    public void testCrossJoinEmptyBuildPage()
            throws Exception
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 100) a " +
                "CROSS JOIN (SELECT * FROM lineitem WHERE orderkey < 0) b");
    }

    @Test
    public void testSimpleCrossJoins()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT 1 a) x CROSS JOIN (SELECT 2 b) y");
    }

    @Test
    public void testCrossJoinsWithWhereClause()
            throws Exception
    {
        assertQuery("" +
                        "SELECT a, b, c, d " +
                        "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) t1 (a, b) " +
                        "CROSS JOIN (VALUES (1, 1.1), (3, 3.3), (5, 5.5)) t2 (c, d) " +
                        "WHERE t1.a > t2.c",
                "SELECT * FROM (VALUES  (2, 'b', 1, 1.1), (3, 'c', 1, 1.1), (4, 'd', 1, 1.1), (4, 'd', 3, 3.3))");
    }

    @Test
    public void testCrossJoinsDifferentDataTypes()
            throws Exception
    {
        assertQuery("" +
                "SELECT * " +
                "FROM (SELECT 'AAA' a1, 11 b1, 33.3 c1, true as d1, 21 e1) x " +
                "CROSS JOIN (SELECT 4444.4 a2, false as b2, 'BBB' c2, 22 d2) y");
    }

    @Test
    public void testCrossJoinWithNulls() throws Exception
    {
        assertQuery("SELECT a, b FROM (VALUES (1), (2)) t (a) CROSS JOIN (VALUES (1), (3)) u (b)",
                "SELECT * FROM (VALUES  (1, 1), (1, 3), (2, 1), (2, 3))");
        assertQuery("SELECT a, b FROM (VALUES (1), (2), (null)) t (a), (VALUES (11), (null), (13)) u (b)",
                "SELECT * FROM (VALUES (1, 11), (1, null), (1, 13), (2, 11), (2, null), (2, 13), (null, 11), (null, null), (null, 13))");
        assertQuery("SELECT a, b FROM (VALUES ('AA'), ('BB'), (null)) t (a), (VALUES ('111'), (null), ('333')) u (b)",
                "SELECT * FROM (VALUES ('AA', '111'), ('AA', null), ('AA', '333'), ('BB', '111'), ('BB', null), ('BB', '333'), (null, '111'), (null, null), (null, '333'))");
    }

    @Test
    public void testImplicitCrossJoin()
            throws Exception
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 3) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 4) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 2) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) b, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) c ");

        // Inner Join converted to cross join because all join conditions are pushed down.
        assertQuery("" +
                "SELECT l.orderkey, l.linenumber " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT o.custkey " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");
    }

    @Test
    public void testCrossJoinUnion()
            throws Exception
    {
        assertQuery("" +
                "SELECT t.c " +
                "FROM (SELECT 1) " +
                "CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t");
        assertQuery("" +
                "SELECT a, b " +
                "FROM (VALUES (1, 1)) " +
                "CROSS JOIN (SELECT 0 AS a, 0 AS b UNION ALL SELECT 1, 1) t");
    }

    @Test
    public void testCrossJoinUnnestWithUnion()
            throws Exception
    {
        assertQuery("" +
                "SELECT col, COUNT(*)\n" +
                "FROM ((\n" +
                "    SELECT ARRAY[1, 2] AS a\n" +
                "    UNION ALL\n" +
                "    SELECT ARRAY[1, 3] AS a)  unionresult\n" +
                "  CROSS JOIN UNNEST(unionresult.a) t(col))\n" +
                "GROUP BY col",
                "SELECT * FROM VALUES (1, 2), (2, 1), (3, 1)");
    }

    @Test
    public void testJoinOnConstantExpression()
            throws Exception
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "   JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) b " +
                "   ON 123 = 123");
    }

    @Test
    public void testSemiJoin()
            throws Exception
    {
        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) IN (SELECT orderkey FROM orders WHERE orderkey > 1)");
        //
        // constant literal versus a subquery
        assertQuery("SELECT 10 in (SELECT orderkey FROM orders)");

        // the same IN subquery used twice
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (2,2), (3, 3)) t(x, y) WHERE (x+y in (VALUES 4, 5)) AND (x*y in (VALUES 4, 5))",
                "VALUES (2,2)");

        // Throw in a bunch of IN subquery predicates
        assertQuery("" +
                "SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 4 = 0),\n" +
                "  SUM(\n" +
                "    CASE\n" +
                "      WHEN orderkey\n" +
                "        IN (\n" +
                "          SELECT orderkey\n" +
                "          FROM lineitem\n" +
                "          WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END)\n" +
                "FROM orders\n" +
                "GROUP BY orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 4 = 0)\n" +
                "HAVING SUM(\n" +
                "  CASE\n" +
                "    WHEN orderkey\n" +
                "      IN (\n" +
                "        SELECT orderkey\n" +
                "        FROM lineitem\n" +
                "        WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END) > 1");
    }

    @Test
    public void testJoinConstantPropagation()
            throws Exception
    {
        assertQuery("" +
                "SELECT x, y, COUNT(*)\n" +
                "FROM (SELECT orderkey, 0 AS x FROM orders) a \n" +
                "JOIN (SELECT orderkey, 1 AS y FROM orders) b \n" +
                "ON a.orderkey = b.orderkey\n" +
                "GROUP BY 1, 2");
    }

    @Test
    public void testAntiJoin()
            throws Exception
    {
        assertQuery("" +
                "SELECT *, orderkey\n" +
                "  NOT IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 3 = 0)\n" +
                "FROM orders");
    }

    @Test
    public void testSemiJoinLimitPushDown()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 2 = 0)\n" +
                "  FROM orders\n" +
                "  LIMIT 10)");
    }

    @Test
    public void testSemiJoinNullHandling()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem)\n" +
                "FROM orders");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 4 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
    }

    @Test
    public void testSemiJoinWithGroupBy()
            throws Exception
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT sum(orderkey) FROM orders WHERE orderkey < 5)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 5)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");
    }

    @Test
    public void testSemiJoinUnionNullHandling()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 300 = 0\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testSemiJoinAggregationNullHandling()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 10 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 2 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0)");
    }

    @Test
    public void testSemiJoinUnionAggregationNullHandling()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 250 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 300 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 100 = 0)\n");
    }

    @Test
    public void testSemiJoinAggregationUnionNullHandling()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM (\n" +
                "      SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 200 = 0\n" +
                "      UNION ALL\n" +
                "      SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 300 = 0\n" +
                "    )\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testSameInPredicateInProjectionAndFilter()
            throws Exception
    {
        assertQuery("SELECT x IN (SELECT * FROM (VALUES 1))\n" +
                        "FROM (VALUES 1) t(x)\n" +
                        "WHERE x IN (SELECT * FROM (VALUES 1))",
                "SELECT 1");

        assertQuery("SELECT x IN (SELECT * FROM (VALUES 1))\n" +
                "FROM (VALUES 2) t(x)\n" +
                "WHERE x IN (SELECT * FROM (VALUES 1))",
                "SELECT 1 WHERE false");
    }

    @Test
    public void testScalarSubquery()
            throws Exception
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is null");
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT (SELECT 1), (SELECT 2), (SELECT 3)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        for (String joinType : ImmutableList.of("INNER", "LEFT OUTER")) {
            assertQuery("SELECT l.orderkey, COUNT(*) " +
                    "FROM lineitem l " + joinType + " JOIN orders o ON l.orderkey = o.orderkey " +
                    "WHERE l.orderkey BETWEEN" +
                    "   (SELECT avg(orderkey) FROM orders) - 10 " +
                    "   AND" +
                    "   (SELECT avg(orderkey) FROM orders) + 10 " +
                    "GROUP BY l.orderkey");
        }

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "Scalar sub-query has returned multiple rows";
        assertQueryFails("SELECT * FROM lineitem WHERE orderkey = (\n" +
                "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");
    }

    @Test
    public void testExistsSubquery()
            throws Exception
    {
        // nested
        assertQuery("SELECT EXISTS(SELECT NOT EXISTS(SELECT EXISTS(SELECT 1)))");

        // aggregation
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "EXISTS(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "NOT EXISTS(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "NOT EXISTS(SELECT orderkey FROM orders WHERE false)");

        // no output
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "EXISTS(SELECT orderkey FROM orders WHERE false)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "NOT EXISTS(SELECT orderkey FROM orders WHERE false)");

        // exists with in-predicate
        assertQuery("SELECT (EXISTS(SELECT 1)) IN (false)", "SELECT false");
        assertQuery("SELECT (NOT EXISTS(SELECT 1)) IN (false)", "SELECT true");

        assertQuery("SELECT (EXISTS(SELECT 1)) IN (true, false)", "SELECT true");
        assertQuery("SELECT (NOT EXISTS(SELECT 1)) IN (true, false)", "SELECT true");

        assertQuery("SELECT (EXISTS(SELECT 1 WHERE false)) IN (true, false)", "SELECT true");
        assertQuery("SELECT (NOT EXISTS(SELECT 1 WHERE false)) IN (true, false)", "SELECT true");

        assertQuery("SELECT (EXISTS(SELECT 1 WHERE false)) IN (false)", "SELECT true");
        assertQuery("SELECT (NOT EXISTS(SELECT 1 WHERE false)) IN (false)", "SELECT false");

        // multiple exists
        assertQuery("SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 1)) WHERE NOT EXISTS(SELECT 1)", "SELECT true WHERE false");
        assertQuery("SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 3)) WHERE NOT EXISTS(SELECT 1 WHERE false)", "SELECT true");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(EXISTS(SELECT min(orderkey) FROM orders))" +
                "=" +
                "(NOT EXISTS(SELECT orderkey FROM orders WHERE false))",
                "SELECT count(*) FROM lineitem");
        assertQuery("SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE EXISTS(SELECT avg(orderkey) FROM orders)");

        // subqueries with joins
        for (String joinType : ImmutableList.of("INNER", "LEFT OUTER")) {
            assertQuery("SELECT l.orderkey, COUNT(*) " +
                    "FROM lineitem l " + joinType + " JOIN orders o ON l.orderkey = o.orderkey " +
                    "WHERE EXISTS(SELECT avg(orderkey) FROM orders) " +
                    "GROUP BY l.orderkey");
        }

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY EXISTS(SELECT 2)");
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY NOT(EXISTS(SELECT 2))");
    }

    @Test
    public void testScalarSubqueryWithGroupBy()
            throws Exception
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT max(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT max(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) < (SELECT avg(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "HAVING min(orderkey) < (SELECT max(orderkey) FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT sum(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, max(orderkey), (SELECT min(orderkey) FROM orders WHERE orderkey < 5)" +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING sum(orderkey) > (SELECT min(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT count(orderkey) FROM orders WHERE orderkey < 7)" +
                "HAVING min(orderkey) < (SELECT sum(orderkey) FROM orders WHERE orderkey < 7)");
    }

    @Test
    public void testOutputInEnforceSingleRow()
            throws Exception
    {
        assertQuery("SELECT count(*) FROM (SELECT (SELECT 1))");
        assertQuery("SELECT * FROM (SELECT (SELECT 1))");
        assertQueryFails(
                "SELECT * FROM (SELECT (SELECT 1, 2))",
                "line 1:23: Multiple columns returned by subquery are not yet supported. Found 2");
    }

    @Test
    public void testExistsSubqueryWithGroupBy()
            throws Exception
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 17)");

        assertQuery("SELECT linenumber, max(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 5)" +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 17)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 17)" +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 27)");
    }

    @Test
    public void testCorrelatedScalarSubqueries()
            throws Exception
    {
        String errorMsg = "line .*: Correlated queries not yet supported. Invalid column reference: .*";

        assertQueryFails("SELECT (SELECT l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l WHERE 1 = (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY (SELECT l.orderkey)", errorMsg);

        // group by
        assertQueryFails("SELECT max(l.quantity), l.orderkey, (SELECT l.orderkey) FROM lineitem l GROUP BY l.orderkey", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) < (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey, (SELECT l.orderkey)", errorMsg);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON l1.orderkey= (SELECT l2.orderkey)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l1 WHERE 1 = (SELECT count(*) FROM lineitem l2 CROSS JOIN (SELECT l1.orderkey) l3)", errorMsg);

        // subrelation
        assertQueryFails("SELECT * FROM lineitem l WHERE l.orderkey= (SELECT * FROM (SELECT l.orderkey))", errorMsg);
    }

    @Test
    public void testCorrelatedInPredicateSubqueries()
    {
        String errorMsg = "line .*: Correlated queries not yet supported. Invalid column reference: .*";

        assertQueryFails("SELECT 1 IN (SELECT l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l WHERE 1 IN (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY 1 IN (SELECT l.orderkey)", errorMsg);

        // group by
        assertQueryFails("SELECT max(l.quantity), l.orderkey, 1 IN (SELECT l.orderkey) FROM lineitem l GROUP BY l.orderkey", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) IN (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey, 1 IN (SELECT l.orderkey)", errorMsg);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON l1.orderkey IN (SELECT l2.orderkey)", errorMsg);

        // subrelation
        assertQueryFails("SELECT * FROM lineitem l WHERE l.orderkey = (SELECT * FROM (SELECT 1 IN (SELECT l.orderkey)))", errorMsg);
    }

    @Test
    public void testCorrelatedExistsSubqueries()
            throws Exception
    {
        String errorMsg = "line .*: Correlated queries not yet supported. Invalid column reference: .*";

        assertQueryFails("SELECT EXISTS(SELECT l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l WHERE EXISTS(SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY EXISTS(SELECT l.orderkey)", errorMsg);

        // group by
        assertQueryFails("SELECT max(l.quantity), l.orderkey, EXISTS(SELECT l.orderkey) FROM lineitem l GROUP BY l.orderkey", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING EXISTS (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey, EXISTS (SELECT l.orderkey)", errorMsg);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON EXISTS(SELECT l1.orderkey = l2.orderkey)", errorMsg);

        // subrelation
        assertQueryFails("SELECT * FROM lineitem l WHERE l.orderkey= (SELECT * FROM (SELECT EXISTS( SELECT l.orderkey)))", errorMsg);
    }

    @Test
    public void testPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey+1 as a FROM orders WHERE orderstatus = 'F' UNION ALL \n" +
                "  SELECT orderkey FROM orders WHERE orderkey % 2 = 0 UNION ALL \n" +
                "  (SELECT orderkey+custkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                ") \n" +
                "WHERE a < 20 OR a > 100 \n" +
                "ORDER BY a");
    }

    @Test
    public void testJoinPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "JOIN (\n" +
                "  SELECT * FROM orders\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND lineitem.suppkey > orders.orderkey");
    }

    @Test
    public void testLeftJoinAsInnerPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainLeftJoinPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithSelfEquality()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithNullConstant()
            throws Exception
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM orders a\n" +
                "LEFT OUTER JOIN orders b\n" +
                "  ON a.clerk = b.clerk\n" +
                "WHERE a.orderpriority='5-LOW'\n" +
                "  AND b.orderpriority='1-URGENT'\n" +
                "  AND b.clerk is null\n" +
                "  AND a.orderkey % 4 = 0\n");
    }

    @Test
    public void testRightJoinAsInnerPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders\n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainRightJoinPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testRightJoinPredicatePushdownWithSelfEquality()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testPredicatePushdownJoinEqualityGroups()
            throws Exception
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT custkey custkey1, custkey%4 custkey1a, custkey%8 custkey1b, custkey%16 custkey1c\n" +
                "  FROM orders\n" +
                ") orders1 \n" +
                "JOIN (\n" +
                "  SELECT custkey custkey2, custkey%4 custkey2a, custkey%8 custkey2b\n" +
                "  FROM orders\n" +
                ") orders2 ON orders1.custkey1 = orders2.custkey2\n" +
                "WHERE custkey2a = custkey2b\n" +
                "  AND custkey1 = custkey1a\n" +
                "  AND custkey2 = custkey2a\n" +
                "  AND custkey1a = custkey1c\n" +
                "  AND custkey1b = custkey1c\n" +
                "  AND custkey1b % 2 = 0");
    }

    @Test
    public void testGroupByKeyPredicatePushdown()
            throws Exception
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT custkey1, orderstatus1, SUM(totalprice1) totalprice, MAX(custkey2) maxcustkey\n" +
                "  FROM (\n" +
                "    SELECT *\n" +
                "    FROM (\n" +
                "      SELECT custkey custkey1, orderstatus orderstatus1, CAST(totalprice AS BIGINT) totalprice1, orderkey orderkey1\n" +
                "      FROM orders\n" +
                "    ) orders1 \n" +
                "    JOIN (\n" +
                "      SELECT custkey custkey2, orderstatus orderstatus2, CAST(totalprice AS BIGINT) totalprice2, orderkey orderkey2\n" +
                "      FROM orders\n" +
                "    ) orders2 ON orders1.orderkey1 = orders2.orderkey2\n" +
                "  ) \n" +
                "  GROUP BY custkey1, orderstatus1\n" +
                ")\n" +
                "WHERE custkey1 = maxcustkey\n" +
                "AND maxcustkey % 2 = 0 \n" +
                "AND orderstatus1 = 'F'\n" +
                "AND totalprice > 10000\n" +
                "ORDER BY custkey1, orderstatus1, totalprice, maxcustkey");
    }

    @Test
    public void testNonDeterministicJoinPredicatePushdown()
            throws Exception
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT DISTINCT *\n" +
                "  FROM (\n" +
                "    SELECT 'abc' as col1a, 500 as col1b FROM lineitem limit 1\n" +
                "  ) table1\n" +
                "  JOIN (\n" +
                "    SELECT 'abc' as col2a FROM lineitem limit 1000000\n" +
                "  ) table2\n" +
                "  ON table1.col1a = table2.col2a\n" +
                "  WHERE rand() * 1000 > table1.col1b\n" +
                ")");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000000);
    }

    @Test
    public void testTrivialNonDeterministicPredicatePushdown()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) WHERE rand() >= 0");
    }

    @Test
    public void testNonDeterministicTableScanPredicatePushdown()
            throws Exception
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  LIMIT 1000\n" +
                ")\n" +
                "WHERE rand() > 0.5");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000);
    }

    @Test
    public void testNonDeterministicAggregationPredicatePushdown()
            throws Exception
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey, COUNT(*)\n" +
                "  FROM lineitem\n" +
                "  GROUP BY orderkey\n" +
                "  LIMIT 1000\n" +
                ")\n" +
                "WHERE rand() > 0.5");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000);
    }

    @Test
    public void testSemiJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 2 = 0 AND orderkey % 3 = 0)\n" +
                "WHERE orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 7 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 2 = 0)\n" +
                "  AND\n" +
                "    orderkey % 2 = 0");
    }

    @Test
    public void testUnionAllPredicateMoveAroundWithOverlappingProjections()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey AS x, orderkey as y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0\n" +
                "  UNION ALL\n" +
                "  SELECT orderkey AS x, orderkey as y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 2 = 0\n" +
                ") a\n" +
                "JOIN (\n" +
                "  SELECT orderkey AS x, orderkey as y\n" +
                "  FROM orders\n" +
                ") b\n" +
                "ON a.x = b.x");
    }

    @Test
    public void testTableSampleBernoulliBoundaryValues()
            throws Exception
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (0)");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", fullSample.getTypes());

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testTableSampleBernoulli()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        int total = computeExpected("SELECT orderkey FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().size();

        for (int i = 0; i < 100; i++) {
            List<MaterializedRow> values = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE BERNOULLI (50)").getMaterializedRows();

            assertEquals(values.size(), ImmutableSet.copyOf(values).size(), "TABLESAMPLE produced duplicate rows");
            stats.addValue(values.size() * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test
    public void testTableSamplePoissonized()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        long total = (long) computeExpected("SELECT COUNT(*) FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().get(0).getField(0);

        for (int i = 0; i < 100; i++) {
            String value = (String) computeActual("SELECT COUNT(*) FROM orders TABLESAMPLE POISSONIZED (50) APPROXIMATE AT 95 CONFIDENCE").getMaterializedRows().get(0).getField(0);
            stats.addValue(Long.parseLong(value.split(" ")[0]) * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test
    public void testTableSamplePoissonizedRescaled()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        long total = (long) computeExpected("SELECT COUNT(*) FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().get(0).getField(0);

        for (int i = 0; i < 100; i++) {
            String value = (String) computeActual("SELECT COUNT(*) FROM orders TABLESAMPLE POISSONIZED (50) RESCALED APPROXIMATE AT 95 CONFIDENCE").getMaterializedRows().get(0).getField(0);
            stats.addValue(Long.parseLong(value.split(" ")[0]) * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.90 && mean < 1.1, format("Expected sample to be rescaled to ~1.0, but was %s", mean));
        assertTrue(stats.getVariance() > 0, "Samples all had the exact same size");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:8: Unexpected parameters (integer) for function length. Expected:\\E.*")
    public void testFunctionNotRegistered()
    {
        computeActual("SELECT length(1)");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:8: Unexpected parameters (color) for function greatest. Expected: greatest(E) E:orderable\\E.*")
    public void testFunctionArgumentTypeConstraint()
    {
        computeActual("SELECT greatest(rgb(255, 0, 0))");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:10: '<>' cannot be applied to integer, varchar(1)\\E")
    public void testTypeMismatch()
    {
        computeActual("SELECT 1 <> 'x'");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:8: Unknown type: ARRAY(FOO)\\E")
    public void testInvalidType()
    {
        computeActual("SELECT CAST(null AS array(foo))");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:21: '+' cannot be applied to varchar, integer\\E")
    public void testInvalidTypeInfixOperator()
    {
        // Comment on why error message references varchar(214783647) instead of varchar(2) which seems expected result type for concatenation in expression.
        // Currently variable argument functions do not play well with arguments using parametrized types.
        // The variable argument functions mechanism requires that all the arguments are of exactly same type. We cannot enforce that base must match but parameters may differ.
        computeActual("SELECT ('a' || 'z') + (3 * 4) / 5");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:12: Cannot check if varchar(1) is BETWEEN integer and varchar(1)\\E")
    public void testInvalidTypeBetweenOperator()
    {
        computeActual("SELECT 'a' BETWEEN 3 AND 'z'");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\Qline 1:20: All ARRAY elements must be the same type: integer\\E")
    public void testInvalidTypeArray()
    {
        computeActual("SELECT ARRAY[1, 2, 'a']");
    }

    @Test
    public void testTimeLiterals()
            throws Exception
    {
        MaterializedResult.Builder builder = resultBuilder(getSession(), DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE);

        DateTimeZone sessionTimeZone = DateTimeZoneIndex.getDateTimeZone(getSession().getTimeZoneKey());
        DateTimeZone utcPlus6 = DateTimeZoneIndex.getDateTimeZone(TimeZoneKey.getTimeZoneKeyForOffset(6 * 60));

        builder.row(
                new Date(new DateTime(2013, 3, 22, 0, 0, sessionTimeZone).getMillis()),
                new Time(new DateTime(1970, 1, 1, 3, 4, 5, sessionTimeZone).getMillisOfDay()),
                new Time(new DateTime(1970, 1, 1, 3, 4, 5, utcPlus6).getMillis()), // hack because java.sql.Time compares based on actual number of ms since epoch instead of ms since midnight
                new Timestamp(new DateTime(1960, 1, 22, 3, 4, 5, sessionTimeZone).getMillis()),
                new Timestamp(new DateTime(1960, 1, 22, 3, 4, 5, utcPlus6).getMillis()));

        MaterializedResult actual = computeActual("SELECT DATE '2013-03-22', TIME '3:04:05', TIME '3:04:05 +06:00', TIMESTAMP '1960-01-22 3:04:05', TIMESTAMP '1960-01-22 3:04:05 +06:00'");

        assertEquals(actual, builder.build());
    }

    @Test
    public void testNonReservedTimeWords()
            throws Exception
    {
        assertQuery("" +
                "SELECT TIME, TIMESTAMP, DATE, INTERVAL\n" +
                "FROM (SELECT 1 TIME, 2 TIMESTAMP, 3 DATE, 4 INTERVAL)");
    }

    @Test
    public void testCustomAdd()
            throws Exception
    {
        assertQuery(
                "SELECT custom_add(orderkey, custkey) FROM orders",
                "SELECT orderkey + custkey FROM orders");
    }

    @Test
    public void testCustomSum()
            throws Exception
    {
        @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
        assertQuery(sql, sql.replace("custom_sum", "sum"));
    }

    @Test
    public void testCustomRank()
            throws Exception
    {
        @Language("SQL") String sql = "" +
                "SELECT orderstatus, clerk, sales\n" +
                ", custom_rank() OVER (PARTITION BY orderstatus ORDER BY sales DESC) rnk\n" +
                "FROM (\n" +
                "  SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                "  FROM orders\n" +
                "  GROUP BY orderstatus, clerk\n" +
                ")\n" +
                "ORDER BY orderstatus, clerk";

        assertEquals(computeActual(sql), computeActual(sql.replace("custom_rank", "rank")));
    }

    @Test
    public void testApproxSetBigint()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarchar()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS VARCHAR))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1024L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDouble()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS DOUBLE))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1014L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetBigintGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(custkey)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001L)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarcharGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS VARCHAR))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1021L)
                .row("F", 1019L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDoubleGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS DOUBLE))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1011L)
                .row("F", 1011L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(IF(orderstatus = 'O', custkey))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1001L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetOnlyNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(null)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(new Object[] { null })
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(orderstatus != 'O', custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetGroupByWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(custkey % 2 <> 0, custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 499L)
                .row("F", 496L)
                .row("P", 153L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLog()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(create_hll(custkey))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001L)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1001L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogOnlyNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(null)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(new Object[] { null })
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetBigint()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetVarchar()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1024L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetDouble()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1014L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetBigintGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001L)
                .row("F", 998L)
                .row("P", 308L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetVarcharGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1021L)
                .row("F", 1019L)
                .row("P", 302L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetDoubleGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1011L)
                .row("F", 1011L)
                .row("P", 306L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(IF(orderstatus = 'O', custkey)) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1001L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetOnlyNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(null) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(new Object[] { null })
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(IF(orderstatus != 'O', custkey)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998L)
                .row("P", 308L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetGroupByWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(IF(custkey % 2 <> 0, custkey)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 495L)
                .row("F", 491L)
                .row("P", 153L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }
    @Test
    public void testValuesWithNonTrivialType()
            throws Exception
    {
        MaterializedResult actual = computeActual("VALUES (0.0/0.0, 1.0/0.0, -1.0/0.0)");

        List<MaterializedRow> rows = actual.getMaterializedRows();
        assertEquals(rows.size(), 1);

        MaterializedRow row = rows.get(0);
        assertTrue(((Double) row.getField(0)).isNaN());
        assertEquals(row.getField(1), Double.POSITIVE_INFINITY);
        assertEquals(row.getField(2), Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testValuesWithTimestamp()
            throws Exception
    {
        MaterializedResult actual = computeActual("VALUES (current_timestamp, now())");

        List<MaterializedRow> rows = actual.getMaterializedRows();
        assertEquals(rows.size(), 1);

        MaterializedRow row = rows.get(0);
        assertEquals(row.getField(0), row.getField(1));
    }

    @Test
    public void testValuesWithUnusedColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT foo from (values (1, 2)) a(foo, bar)");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testFilterPushdownWithAggregation()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders) WHERE 0=1");
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders) WHERE null");
    }

    @Test
    public void testAccessControl()
            throws Exception
    {
        assertAccessDenied("SELECT COUNT(true) FROM orders", "Cannot select from table .*.orders.*", privilege("orders", SELECT_TABLE));
        assertAccessDenied("INSERT INTO orders SELECT * FROM orders", "Cannot insert into table .*.orders.*", privilege("orders", INSERT_TABLE));
        assertAccessDenied("DELETE FROM orders", "Cannot delete from table .*.orders.*", privilege("orders", DELETE_TABLE));
        assertAccessDenied("CREATE TABLE foo AS SELECT * FROM orders", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
    }

    @Test
    public void testCoercions()
            throws Exception
    {
        // VARCHAR
        assertQuery("SELECT length(NULL)");
        assertQuery("SELECT CAST('abc' AS VARCHAR(255)) || CAST('abc' AS VARCHAR(252))");
        assertQuery("SELECT CAST('abc' AS VARCHAR(255)) || 'abc'");

        // DECIMAL - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + NULL");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) + CAST(292.1 AS DECIMAL(5,1))");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS DECIMAL(5,1))"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(5,1))]"));

        // BIGINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(292 AS BIGINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(292 AS BIGINT)");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS BIGINT)"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(19,0))]"));

        // DECIMAL - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(1.1 AS DOUBLE)");
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) = CAST(1.1 AS DOUBLE)");
        assertQuery("SELECT SIN(CAST(1.1 AS DECIMAL(38,1)))");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282.1 AS DOUBLE), CAST(283.2 AS DOUBLE)] || CAST(101.3 AS DECIMAL(5,1))"),
                computeActual("SELECT ARRAY[CAST(282.1 AS DOUBLE), CAST(283.2 AS DOUBLE), CAST(101.3 AS DOUBLE)]"));

        // INTEGER - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(292 AS INTEGER)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(292 AS INTEGER)");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS INTEGER)"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(19,0))]"));

        // TINYINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(CAST(121 AS DECIMAL(30,1)) AS TINYINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(CAST(121 AS DECIMAL(30,1)) AS TINYINT)");

        // SMALLINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(CAST(121 AS DECIMAL(30,1)) AS SMALLINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(CAST(121 AS DECIMAL(30,1)) AS SMALLINT)");

        // Complex coercions across joins
        assertQuery("SELECT * FROM (" +
                "  SELECT t2.x || t2.z cc FROM (" +
                "    SELECT *" +
                "    FROM (VALUES (CAST('a' as VARCHAR), CAST('c' as VARCHAR))) t(x, z)" +
                "  ) t2" +
                "  JOIN (" +
                "    SELECT *" +
                "    FROM (VALUES (CAST('a' as VARCHAR), CAST('c' as VARCHAR))) u(x, z)" +
                "    WHERE z='c'" +
                "  ) u2" +
                "  ON t2.z = u2.z" +
                ") tt " +
                "WHERE cc = 'ac'",
                "SELECT 'ac'");

        assertQuery("SELECT * FROM (" +
                "  SELECT greatest (t.x, t.z) cc FROM (" +
                "    SELECT *" +
                "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) t(x, z)" +
                "  ) t" +
                "  JOIN (" +
                "    SELECT *" +
                "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) u(x, z)" +
                "    WHERE z='c'" +
                "  ) u" +
                "  ON t.z = u.z" +
                ")" +
                "WHERE cc = 'c'",
                "SELECT 'c'");

        assertQuery("SELECT cc[1], cc[2] FROM (" +
                " SELECT * FROM (" +
                "  SELECT array[t.x, t.z] cc FROM (" +
                "    SELECT *" +
                "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) t(x, z)" +
                "  ) t" +
                "  JOIN (" +
                "    SELECT *" +
                "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) u(x, z)" +
                "    WHERE z='c'" +
                "  ) u" +
                "  ON t.z = u.z)" +
                " WHERE cc = array['a', 'c'])",
                "SELECT 'a', 'c'");

        assertQuery("SELECT c = 'x'" +
                "FROM (" +
                "    SELECT 'x' as c" +
                "    UNION ALL" +
                "    SELECT 'yy' as c" +
                ")");
    }

    @Test
    public void testExecute() throws Exception
    {
        Session session = getSession().withPreparedStatement("my_query", "SELECT 123, 'abc'");
        assertQuery(session, "EXECUTE my_query", "SELECT 123, 'abc'");
    }

    @Test
    public void testExecuteNoSuchQuery()
    {
        assertQueryFails("EXECUTE my_query", "Prepared statement not found: my_query");
    }
}
