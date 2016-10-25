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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.STANDALONE_LAMBDA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WINDOW_REQUIRES_OVER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestAnalyzer
{
    private static final String TPCH_CATALOG = "tpch";
    private static final ConnectorId TPCH_CONNECTOR_ID = new ConnectorId(TPCH_CATALOG);
    private static final String SECOND_CATALOG = "c2";
    private static final ConnectorId SECOND_CONNECTOR_ID = new ConnectorId(SECOND_CATALOG);
    private static final String THIRD_CATALOG = "c3";
    private static final ConnectorId THIRD_CONNECTOR_ID = new ConnectorId(THIRD_CATALOG);
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();
    private static final Session CLIENT_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;

    @Test
    public void testNonComparableGroupBy()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1)) GROUP BY 1");
    }

    @Test
    public void testNonComparableWindowPartition()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (PARTITION BY t.x) FROM (VALUES(CAST (NULL AS HyperLogLog))) AS t(x)");
    }

    @Test
    public void testNonComparableWindowOrder()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (ORDER BY t.x) FROM (VALUES(color('red'))) AS t(x)");
    }

    @Test
    public void testNonComparableDistinctAggregation()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT count(DISTINCT x) FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testNonComparableDistinct()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT * FROM (SELECT approx_set(1) x)");
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT x FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testInSubqueryTypes()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES 'a') t(y) WHERE y IN (VALUES 1)");
        assertFails(TYPE_MISMATCH, "SELECT (VALUES true) IN (VALUES 1)");
    }

    @Test
    public void testScalarSubQuery()
            throws Exception
    {
        analyze("SELECT 'a', (VALUES 1) GROUP BY 1");
        analyze("SELECT 'a', (SELECT (1))");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) = 2");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (VALUES 1)");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (2)");
        analyze("SELECT * FROM (SELECT 1) t1(x) WHERE x IN (SELECT 1)");
    }

    @Test
    public void testHavingReferencesOutputAlias()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT sum(a) x FROM t1 HAVING x > 5");
    }

    @Test
    public void testWildcardWithInvalidPrefix()
            throws Exception
    {
        assertFails(MISSING_TABLE, "SELECT foo.* FROM t1");
    }

    @Test
    public void testGroupByWithWildcard()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 USING (a) GROUP BY u1.a, u2.a, 3");
    }

    @Test
    public void testGroupByInvalidOrdinal()
            throws Exception
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 0");
    }

    @Test
    public void testOrderByInvalidOrdinal()
            throws Exception
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 0");
    }

    @Test
    public void testOrderByNonComparable()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY x");
    }

    @Test
    public void testNestedAggregation()
            throws Exception
    {
        assertFails(NESTED_AGGREGATION, "SELECT sum(count(*)) FROM t1");
    }

    @Test
    public void testAggregationsNotAllowed()
            throws Exception
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 WHERE sum(a) > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 GROUP BY sum(a)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a");
    }

    @Test
    public void testWindowsNotAllowed()
            throws Exception
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 WHERE foo() over () > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 GROUP BY rank() over ()");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a");
    }

    @Test
    public void testInvalidTable()
            throws Exception
    {
        assertFails(MISSING_CATALOG, "SELECT * FROM foo.bar.t");
        assertFails(MISSING_SCHEMA, "SELECT * FROM foo.t");
        assertFails(MISSING_TABLE, "SELECT * FROM foo");
    }

    @Test
    public void testInvalidSchema()
            throws Exception
    {
        assertFails(MISSING_SCHEMA, "SHOW TABLES FROM NONEXISTENT_SCHEMA");
        assertFails(MISSING_SCHEMA, "SHOW TABLES IN NONEXISTENT_SCHEMA LIKE '%'");
    }

    @Test
    public void testNonAggregate()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 'a', array[b][1] FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1 GROUP BY c");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) FROM t1 ORDER BY a + 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1 GROUP BY a HAVING c > 5");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (PARTITION BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN b PRECEDING AND a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN a PRECEDING AND UNBOUNDED PRECEDING) FROM t1 GROUP BY b");
    }

    @Test
    public void testInvalidAttribute()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT f FROM t1");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 ORDER BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT count(*) FROM t1 GROUP BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 WHERE f > 1");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = "line 1:8: Column 't.y' cannot be resolved")
    public void testInvalidAttributeCorrectErrorMessage()
            throws Exception
    {
        analyze("SELECT t.y FROM (VALUES 1) t(x)");
    }

    @Test
    public void testOrderByMustAppearInSelectWithDistinct()
            throws Exception
    {
        assertFails(ORDER_BY_MUST_BE_IN_SELECT, "SELECT DISTINCT a FROM t1 ORDER BY b");
    }

    @Test
    public void testNonBooleanWhereClause()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE a");
    }

    @Test
    public void testDistinctAggregations()
            throws Exception
    {
        analyze("SELECT COUNT(DISTINCT a), SUM(a) FROM t1");
    }

    @Test
    public void testMultipleDistinctAggregations()
            throws Exception
    {
        analyze("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT a x FROM t1 ORDER BY x + 1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");
    }

    @Test
    public void testOrderByWithWildcard()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testMismatchedColumnAliasCount()
            throws Exception
    {
        assertFails(MISMATCHED_COLUMN_ALIASES, "SELECT * FROM t1 u (x, y)");
    }

    @Test
    public void testJoinOnConstantExpression()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testJoinOnNonBooleanExpression()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 JOIN t2 ON 5");
    }

    @Test
    public void testJoinOnAmbiguousName()
            throws Exception
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT * FROM t1 JOIN t2 ON a = a");
    }

    @Test
    public void testNonEquiOuterJoin()
            throws Exception
    {
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 RIGHT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testNonBooleanHaving()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT sum(a) FROM t1 HAVING sum(a)");
    }

    @Test
    public void testAmbiguousReferenceInOrderBy()
            throws Exception
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, b x FROM t1 ORDER BY x");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        // TODO: validate output
        analyze("SELECT * FROM t1, t2");
    }

    @Test
    public void testNaturalJoinNotSupported()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 NATURAL JOIN t2");
    }

    @Test
    public void testNestedWindowFunctions()
            throws Exception
    {
        assertFails(NESTED_WINDOW, "SELECT avg(sum(a) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT sum(sum(a) OVER ()) OVER () FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (PARTITION BY sum(b) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (ORDER BY sum(b) OVER ()) FROM t1");
    }

    @Test
    public void testWindowFunctionWithoutOverClause()
    {
        assertFails(WINDOW_REQUIRES_OVER, "SELECT row_number()");
        assertFails(WINDOW_REQUIRES_OVER, "SELECT coalesce(lead(a), 0) from (values(0)) t(a)");
    }

    @Test
    public void testInvalidWindowFrame()
            throws Exception
    {
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS UNBOUNDED FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS 2 FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND 5 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE 2 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE BETWEEN 2 PRECEDING AND 5 FOLLOWING)");

        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 0.5 PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 'foo' PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 0.5 FOLLOWING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 'foo' FOLLOWING)");
    }

    @Test
    public void testDistinctInWindowFunctionParameter()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT a, count(DISTINCT b) OVER () FROM t1");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testGroupByWithRowExpression()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT (a, b) FROM t1 GROUP BY a, b");
    }

    @Test
    public void testHaving()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }

    @Test
    public void testWithCaseInsensitiveResolution()
            throws Exception
    {
        // TODO: verify output
        analyze("WITH AB AS (SELECT * FROM t1) SELECT * FROM ab");
    }

    @Test
    public void testStartTransaction()
            throws Exception
    {
        analyze("START TRANSACTION");
        analyze("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        analyze("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        analyze("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        analyze("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        analyze("START TRANSACTION READ ONLY");
        analyze("START TRANSACTION READ WRITE");
        analyze("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY");
        analyze("START TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED");
        analyze("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE");
    }

    @Test
    public void testCommit()
            throws Exception
    {
        analyze("COMMIT");
        analyze("COMMIT WORK");
    }

    @Test
    public void testRollback()
            throws Exception
    {
        analyze("ROLLBACK");
        analyze("ROLLBACK WORK");
    }

    @Test
    public void testExplainAnalyze()
            throws Exception
    {
        analyze("EXPLAIN ANALYZE SELECT * FROM t1");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t6 (a) SELECT b from t6");
        analyze("INSERT INTO t1 SELECT * FROM t1");
        analyze("INSERT INTO t3 SELECT * FROM t3");
        analyze("INSERT INTO t3 SELECT a, b FROM t3");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 VALUES (1, 2)");
        analyze("INSERT INTO t5 (a) VALUES(null)");

        // ignore t5 hidden column
        analyze("INSERT INTO t5 VALUES (1)");

        // fail if hidden column provided
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t5 VALUES (1, 2)");

        // note b is VARCHAR, while a,c,d are BIGINT
        analyze("INSERT INTO t6 (a) SELECT a from t6");
        analyze("INSERT INTO t6 (a) SELECT c from t6");
        analyze("INSERT INTO t6 (a,b,c,d) SELECT * from t6");
        analyze("INSERT INTO t6 (A,B,C,D) SELECT * from t6");
        analyze("INSERT INTO t6 (a,b,c,d) SELECT d,b,c,a from t6");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t6 (a) SELECT b from t6");
        assertFails(MISSING_COLUMN, "INSERT INTO t6 (unknown) SELECT * FROM t6");
        assertFails(DUPLICATE_COLUMN_NAME, "INSERT INTO t6 (a, a) SELECT * FROM t6");
        assertFails(DUPLICATE_COLUMN_NAME, "INSERT INTO t6 (a, A) SELECT * FROM t6");

        // b is bigint, while a is double, coercion from b to a is possible
        analyze("INSERT INTO t7 (b) SELECT (a) FROM t7 ");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t7 (a) SELECT (b) FROM t7");

        // d is array of bigints, while c is array of doubles, coercion from d to c is possible
        analyze("INSERT INTO t7 (d) SELECT (c) FROM t7 ");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t7 (c) SELECT (d) FROM t7 ");

        analyze("INSERT INTO t7 (d) VALUES (ARRAY[null])");

        analyze("INSERT INTO t6 (d) VALUES (1), (2), (3)");
        analyze("INSERT INTO t6 (a,b,c,d) VALUES (1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'c', 3, 3), (4, 'd', 4, 4)");
    }

    @Test
    public void testInvalidInsert()
            throws Exception
    {
        assertFails(MISSING_TABLE, "INSERT INTO foo VALUES (1)");
        assertFails(NOT_SUPPORTED, "INSERT INTO v1 VALUES (1)");

        // fail if inconsistent fields count
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a) VALUES (1), (1, 2)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES (1), (1, 2)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES (1, 2), (1, 2), (1, 2, 3)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES ('a', 'b'), ('a', 'b', 'c')");

        // fail if mismatched column types
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES ('a', 'b'), (1, 'b')");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES ('a', 'b'), ('a', 'b'), (1, 'b')");
    }

    @Test
    public void testDuplicateWithQuery()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     a AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testCaseInsensitiveDuplicateWithQuery()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     A AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testWithForwardReference()
            throws Exception
    {
        assertFails(MISSING_TABLE,
                "WITH a AS (SELECT * FROM b)," +
                        "     b AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testExpressions()
            throws Exception
    {
        // logical not
        assertFails(TYPE_MISMATCH, "SELECT NOT 1 FROM t1");

        // logical and/or
        assertFails(TYPE_MISMATCH, "SELECT 1 AND TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE AND 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 OR TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE OR 1 FROM t1");

        // comparison
        assertFails(TYPE_MISMATCH, "SELECT 1 = 'a' FROM t1");

        // nullif
        assertFails(TYPE_MISMATCH, "SELECT NULLIF(1, 'a') FROM t1");

        // case
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN TRUE THEN 'a' ELSE 1 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN '1' THEN 1 ELSE 2 END FROM t1");

        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 'a' THEN 2 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 1 THEN 2 ELSE 'a' END FROM t1");

        // coalesce
        assertFails(TYPE_MISMATCH, "SELECT COALESCE(1, 'a') FROM t1");

        // cast
        assertFails(TYPE_MISMATCH, "SELECT CAST(date '2014-01-01' AS bigint)");
        assertFails(TYPE_MISMATCH, "SELECT TRY_CAST(date '2014-01-01' AS bigint)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(null AS UNKNOWN)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(1 AS MAP)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(1 AS ARRAY)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(1 AS ROW)");

        // arithmetic unary
        assertFails(TYPE_MISMATCH, "SELECT -'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT +'a' FROM t1");

        // arithmetic addition/subtraction
        assertFails(TYPE_MISMATCH, "SELECT 'a' + 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 + 'a'  FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' - 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 - 'a' FROM t1");

        // like
        assertFails(TYPE_MISMATCH, "SELECT 1 LIKE 'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 'b' ESCAPE 1 FROM t1");

        // extract
        assertFails(TYPE_MISMATCH, "SELECT EXTRACT(DAY FROM 'a') FROM t1");

        // between
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 2 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 0 AND 'b' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 'b' FROM t1");

        // in
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 1 IN ('a')");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1, 'b')");

        // row type
        assertFails(TYPE_MISMATCH, "SELECT t.x.f1 FROM (VALUES 1) t(x)");
        assertFails(TYPE_MISMATCH, "SELECT x.f1 FROM (VALUES 1) t(x)");
    }

    @Test(enabled = false) // TODO: need to support widening conversion for numbers
    public void testInWithNumericTypes()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE 1 IN (1, 2, 3.5)");
    }

    @Test
    public void testWildcardWithoutFrom()
            throws Exception
    {
        assertFails(WILDCARD_WITHOUT_FROM, "SELECT *");
    }

    @Test
    public void testReferenceWithoutFrom()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT dummy");
    }

    @Test
    public void testGroupBy()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByEmpty()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a FROM t1 GROUP BY ()");
    }

    @Test
    public void testSingleGroupingSet()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT SUM(b) FROM t1 GROUP BY ()");
        analyze("SELECT SUM(b) FROM t1 GROUP BY GROUPING SETS (())");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b))");
    }

    @Test
    public void testMultipleGroupingSetMultipleColumns()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b), (c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a, b, GROUPING SETS ((c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a), (c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), ROLLUP (c, d)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), CUBE (c, d)");
    }

    @Test
    public void testAggregateWithWildcard()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 1 not in GROUP BY clause", "SELECT * FROM (SELECT a + 1, b FROM t1) t GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 't.a' not in GROUP BY clause", "SELECT * FROM (SELECT a, b FROM t1) t GROUP BY b ORDER BY 1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 'a' not in GROUP BY clause", "SELECT * FROM (SELECT a, b FROM t1) GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 1 not in GROUP BY clause", "SELECT * FROM (SELECT a + 1, b FROM t1) GROUP BY b ORDER BY 1");
    }

    @Test
    public void testGroupByCase()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE a WHEN 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN 0 ELSE a END, count(*) FROM t1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN a = 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN 0 ELSE a END, count(*) FROM t1");
    }

    @Test
    public void testMismatchedUnionQueries()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT 1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "SELECT a FROM t1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "(SELECT 1) UNION SELECT 'a'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 1, 2 UNION SELECT 1");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 'a' UNION SELECT 'b', 'c'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "TABLE t2 UNION SELECT 'a'");
    }

    @Test
    public void testUnionUnmatchedOrderByAttribute()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "TABLE t2 UNION ALL SELECT c, d FROM t1 ORDER BY c");
    }

    @Test
    public void testGroupByComplexExpressions()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NOT NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(CAST(a AS VARCHAR) LIKE 'a', 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a IN (1, 2, 3) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 1 IN (a, 2, 3) FROM t1 GROUP BY b");
    }

    @Test
    public void testNonNumericTableSamplePercentage()
            throws Exception
    {
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI ('a')");
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (a + 1)");
    }

    @Test
    public void testTableSampleOutOfRange()
            throws Exception
    {
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-1)");
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-101)");
    }

    @Test
    public void testCreateTableAsColumns()
            throws Exception
    {
        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE TABLE test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE TABLE test AS SELECT 1 a, 2 a");
        assertFails(COLUMN_TYPE_UNKNOWN, "CREATE TABLE test AS SELECT null a");
    }

    @Test
    public void testCreateViewColumns()
            throws Exception
    {
        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE VIEW test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE VIEW test AS SELECT 1 a, 2 a");
        assertFails(COLUMN_TYPE_UNKNOWN, "CREATE VIEW test AS SELECT null a");
    }

    @Test
    public void testShowCreateView()
    {
        analyze("SHOW CREATE VIEW v1");
        analyze("SHOW CREATE VIEW v2");

        assertFails(NOT_SUPPORTED, "SHOW CREATE VIEW t1");
        assertFails(MISSING_TABLE, "SHOW CREATE VIEW none");
    }

    @Test
    public void testStaleView()
            throws Exception
    {
        assertFails(VIEW_IS_STALE, "SELECT * FROM v2");
    }

    @Test
    public void testStoredViewAnalysisScoping()
            throws Exception
    {
        // the view must not be analyzed using the query context
        analyze("WITH t1 AS (SELECT 123 x) SELECT * FROM v1");
    }

    @Test
    public void testStoredViewResolution()
            throws Exception
    {
        // the view must be analyzed relative to its own catalog/schema
        analyze("SELECT * FROM c3.s3.v3");
    }

    @Test
    public void testQualifiedViewColumnResolution()
            throws Exception
    {
        // it should be possible to qualify the column reference with the view name
        analyze("SELECT v1.a FROM v1");
    }

    @Test
    public void testViewWithUppercaseColumn()
            throws Exception
    {
        analyze("SELECT * FROM v4");
    }

    @Test
    public void testUse()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "USE foo");
    }

    @Test
    public void testNotNullInJoinClause()
            throws Exception
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON a.x IS NOT NULL");
    }

    @Test
    public void testIfInJoinClause()
            throws Exception
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON IF(a.x = 1, true, false)");
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertFails(INVALID_LITERAL, "SELECT TIMESTAMP '2012-10-31 01:00:00 PT'");
    }

    @Test
    public void testLambda()
            throws Exception
    {
        analyze("SELECT apply(5, x -> abs(x)) from t1");
        assertFails(STANDALONE_LAMBDA, "SELECT x -> abs(x) from t1");
    }

    @Test
    public void testInvalidDelete()
            throws Exception
    {
        assertFails(MISSING_TABLE, "DELETE FROM foo");
        assertFails(NOT_SUPPORTED, "DELETE FROM v1");
        assertFails(NOT_SUPPORTED, "DELETE FROM v1 WHERE a = 1");
    }

    @Test
    public void testInvalidShowTables()
    {
        assertFails(INVALID_SCHEMA_NAME, "SHOW TABLES FROM a.b.c");

        Session session = testSessionBuilder()
                .setCatalog(null)
                .setSchema(null)
                .build();
        assertFails(session, CATALOG_NOT_SPECIFIED, "SHOW TABLES");
        assertFails(session, CATALOG_NOT_SPECIFIED, "SHOW TABLES FROM a");
        assertFails(session, MISSING_SCHEMA, "SHOW TABLES FROM c2.unknown");

        session = testSessionBuilder()
                .setCatalog(SECOND_CATALOG)
                .setSchema(null)
                .build();
        assertFails(session, SCHEMA_NOT_SPECIFIED, "SHOW TABLES");
        assertFails(session, MISSING_SCHEMA, "SHOW TABLES FROM unknown");
    }

    @Test
    public void testInvalidAtTimeZone()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT 'abc' AT TIME ZONE 'America/Los_Angeles'");
    }

    @Test
    public void testValidJoinOnClause()
            throws Exception
    {
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON TRUE");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1=1");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x=b.x AND a.y=b.y");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON NULL");
    }

    @Test
    public void testInValidJoinOnClause()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x + b.x");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON ROW (TRUE)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON (a.x=b.x, a.y=b.y)");
    }

    @Test
    public void testInvalidAggregationFilter()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT sum(x) FILTER (WHERE x > 1) OVER (PARTITION BY x) FROM (VALUES (1), (2), (2), (4)) t (x)");
        assertFails(NOT_SUPPORTED, "SELECT count(DISTINCT x) FILTER (where y = 1) FROM (VALUES (1, 1)) t(x, y)");
    }

    @Test
    public void testQuantifiedComparisonExpression()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE t1.a <= ALL (VALUES 10, 20)");
        assertFails(MULTIPLE_FIELDS_FROM_SUBQUERY, "SELECT * FROM t1 WHERE t1.a = ANY (SELECT 1, 2)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE t1.a = SOME (VALUES ('abc'))");

        // map is not orderable
        assertFails(TYPE_MISMATCH, ("SELECT map(ARRAY[1], ARRAY['hello']) < ALL (VALUES map(ARRAY[1], ARRAY['hello']))"));
        // but map is comparable
        analyze(("SELECT map(ARRAY[1], ARRAY['hello']) = ALL (VALUES map(ARRAY[1], ARRAY['hello']))"));

        // HLL is neither orderable nor comparable
        assertFails(TYPE_MISMATCH, "SELECT cast(NULL AS HyperLogLog) < ALL (VALUES cast(NULL AS HyperLogLog))");
        assertFails(TYPE_MISMATCH, "SELECT cast(NULL AS HyperLogLog) = ANY (VALUES cast(NULL AS HyperLogLog))");
    }

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager);

        metadata = new MetadataManager(
                new FeaturesConfig(),
                typeManager,
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                transactionManager);

        metadata.getFunctionRegistry().addFunctions(ImmutableList.of(APPLY_FUNCTION));

        catalogManager.registerCatalog(createTestingCatalog(TPCH_CATALOG, TPCH_CONNECTOR_ID));
        catalogManager.registerCatalog(createTestingCatalog(SECOND_CATALOG, SECOND_CONNECTOR_ID));
        catalogManager.registerCatalog(createTestingCatalog(THIRD_CATALOG, THIRD_CONNECTOR_ID));

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table1,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT)))));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table2,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT)))));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table3,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("x", BIGINT, null, true)))));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        inSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG, new ConnectorTableMetadata(table4,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT)))));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table5,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT, null, true)))));

        // table with a varchar column
        SchemaTableName table6 = new SchemaTableName("s1", "t6");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table6,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", VARCHAR),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT)))));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table7 = new SchemaTableName("s1", "t7");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table7,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", DOUBLE),
                        new ColumnMetadata("c", new ArrayType(BIGINT)),
                        new ColumnMetadata("d", new ArrayType(DOUBLE))))));

        // valid view referencing table in same schema
        String viewData1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v1"), viewData1, false));

        // stale view (different column type)
        String viewData2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", VARCHAR)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v2"), viewData2, false));

        // view referencing table in different schema from itself and session
        String viewData3 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t4",
                        Optional.of(SECOND_CATALOG),
                        Optional.of("s2"),
                        ImmutableList.of(new ViewColumn("a", BIGINT)),
                        Optional.of("owner")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(THIRD_CATALOG, "s3", "v3"), viewData3, false));

        // valid view with uppercase column name
        String viewData4 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select A from t1",
                        Optional.of("tpch"),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName("tpch", "s1", "v4"), viewData4, false));

        this.metadata = metadata;
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private static Analyzer createAnalyzer(Session session, Metadata metadata)
    {
        return new Analyzer(
                session,
                metadata,
                SQL_PARSER,
                new AllowAllAccessControl(),
                Optional.empty(),
                emptyList());
    }

    private void analyze(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION, query);
    }

    private void analyze(Session clientSession, @Language("SQL") String query)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata);
                    Statement statement = SQL_PARSER.createStatement(query);
                    analyzer.analyze(statement);
                });
    }

    private void assertFails(SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, query);
    }

    private void assertFails(SemanticErrorCode error, String message, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, message, query);
    }

    private void assertFails(Session session, SemanticErrorCode error, @Language("SQL") String query)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }
        }
    }

    private void assertFails(Session session, SemanticErrorCode error, String message, @Language("SQL") String query)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }

            if (!e.getMessage().equals(message)) {
                fail(format("Expected error '%s', but got '%s'", message, e.getMessage()), e);
            }
        }
    }

    private Catalog createTestingCatalog(String catalogName, ConnectorId connectorId)
    {
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        Connector connector = createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId)));
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
