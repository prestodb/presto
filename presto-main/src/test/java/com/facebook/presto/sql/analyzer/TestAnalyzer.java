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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
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
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
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
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
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
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATION_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_AGGREGATE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.STANDALONE_LAMBDA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TOO_MANY_GROUPING_SETS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ANALYSIS_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_RECURSIVE;
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
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1)) GROUP BY 1");
    }

    @Test
    public void testNonComparableWindowPartition()
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (PARTITION BY t.x) FROM (VALUES(CAST (NULL AS HyperLogLog))) AS t(x)");
    }

    @Test
    public void testNonComparableWindowOrder()
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (ORDER BY t.x) FROM (VALUES(color('red'))) AS t(x)");
    }

    @Test
    public void testNonComparableDistinctAggregation()
    {
        assertFails(TYPE_MISMATCH, "SELECT count(DISTINCT x) FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testNonComparableDistinct()
    {
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT * FROM (SELECT approx_set(1) x)");
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT x FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testInSubqueryTypes()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES 'a') t(y) WHERE y IN (VALUES 1)");
        assertFails(TYPE_MISMATCH, "SELECT (VALUES true) IN (VALUES 1)");
    }

    @Test
    public void testScalarSubQuery()
    {
        analyze("SELECT 'a', (VALUES 1) GROUP BY 1");
        analyze("SELECT 'a', (SELECT (1))");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) = 2");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (VALUES 1)");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (2)");
        analyze("SELECT * FROM (SELECT 1) t1(x) WHERE x IN (SELECT 1)");
    }

    @Test
    public void testReferenceToOutputColumnFromOrderByAggregation()
    {
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT max(a) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY max(a+b)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT DISTINCT a AS a, max(a) AS c from (VALUES (1, 2)) t(a, b) GROUP BY a ORDER BY max(a)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY MAX(a.someField)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(apply(1, z -> x))");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT row_number() over() as a from (values (41, 42), (-41, -42)) t(a,b) group by a+b order by a+b");
    }

    @Test
    public void testHavingReferencesOutputAlias()
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT sum(a) x FROM t1 HAVING x > 5");
    }

    @Test
    public void testWildcardWithInvalidPrefix()
    {
        assertFails(MISSING_TABLE, "SELECT foo.* FROM t1");
    }

    @Test
    public void testGroupByWithWildcard()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 ON u1.a = u2.a GROUP BY u1.a, u2.a, 3");
    }

    @Test
    public void testGroupByInvalidOrdinal()
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 0");
    }

    @Test
    public void testGroupByWithSubquerySelectExpression()
    {
        analyze("SELECT (SELECT t1.a) FROM t1 GROUP BY a");
        analyze("SELECT (SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyze("SELECT (SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:16: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT (SELECT u.a from (values 1) x(a)) FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:16: Subquery uses 'a' which must appear in GROUP BY clause",
                "SELECT (SELECT a+2) FROM t1 GROUP BY a+1");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:36: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT (SELECT 1 FROM t1 WHERE a = u.a) FROM t1 u GROUP BY b");

        // (t1.)a is not part of GROUP BY
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT (SELECT a as a) FROM t1 GROUP BY b");

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyze("SELECT (SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b");
    }

    @Test
    public void testGroupByWithExistsSelectExpression()
    {
        analyze("SELECT EXISTS(SELECT t1.a) FROM t1 GROUP BY a");
        analyze("SELECT EXISTS(SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyze("SELECT EXISTS(SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:22: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT EXISTS(SELECT u.a from (values 1) x(a)) FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:22: Subquery uses 'a' which must appear in GROUP BY clause",
                "SELECT EXISTS(SELECT a+2) FROM t1 GROUP BY a+1");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:42: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT EXISTS(SELECT 1 FROM t1 WHERE a = u.a) FROM t1 u GROUP BY b");

        // (t1.)a is not part of GROUP BY
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT EXISTS(SELECT a as a) FROM t1 GROUP BY b");

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyze("SELECT EXISTS(SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b");
    }

    @Test
    public void testGroupByWithSubquerySelectExpressionWithDereferenceExpression()
    {
        analyze("SELECT (SELECT t.a.someField) " +
                "FROM (VALUES ROW(CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(a, b) " +
                "GROUP BY a");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:16: Subquery uses 't.a' which must appear in GROUP BY clause",
                "SELECT (SELECT t.a.someField) " +
                        "FROM (VALUES ROW(CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(a, b) " +
                        "GROUP BY b");
    }

    @Test
    public void testOrderByInvalidOrdinal()
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 0");
    }

    @Test
    public void testOrderByNonComparable()
    {
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY x");
    }

    @Test
    public void testNestedAggregation()
    {
        assertFails(NESTED_AGGREGATION, "SELECT sum(count(*)) FROM t1");
    }

    @Test
    public void testAggregationsNotAllowed()
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 WHERE sum(a) > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 GROUP BY sum(a)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a");
    }

    @Test
    public void testWindowsNotAllowed()
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 WHERE foo() over () > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 GROUP BY rank() over ()");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a");
        assertFails(NESTED_WINDOW, "SELECT 1 FROM (VALUES 1) HAVING count(*) OVER () > 1");
    }

    @Test
    public void testGrouping()
    {
        analyze("SELECT a, b, sum(c), grouping(a, b) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))");
        analyze("SELECT grouping(t1.a) FROM t1 GROUP BY a");
        analyze("SELECT grouping(b) FROM t1 GROUP BY t1.b");
        analyze("SELECT grouping(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupingNotAllowed()
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT a, b, sum(c) FROM t1 WHERE grouping(a, b) GROUP BY GROUPING SETS ((a), (a, b))");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT a, b, sum(c) FROM t1 GROUP BY grouping(a, b)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT t1.a, t1.b FROM t1 JOIN t2 ON grouping(t1.a, t1.b) > t2.a");

        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT grouping(a) FROM t1");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT * FROM t1 ORDER BY grouping(a)");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT grouping(a) FROM t1 GROUP BY b");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT grouping(a.field) FROM (VALUES ROW(CAST(ROW(1) AS ROW(field BIGINT)))) t(a) GROUP BY a.field");

        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING, "SELECT a FROM t1 GROUP BY a ORDER BY grouping(a)");
    }

    @Test
    public void testGroupingTooManyArguments()
    {
        String grouping = "GROUPING(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a)";
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, %s + 1 FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping));
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, %s as g FROM t1 GROUP BY a, b HAVING g > 0", grouping));
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, rank() OVER (PARTITION BY %s) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping));
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, rank() OVER (PARTITION BY a ORDER BY %s) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping));
    }

    @Test
    public void testInvalidTable()
    {
        assertFails(MISSING_CATALOG, "SELECT * FROM foo.bar.t");
        assertFails(MISSING_SCHEMA, "SELECT * FROM foo.t");
        assertFails(MISSING_TABLE, "SELECT * FROM foo");
    }

    @Test
    public void testInvalidSchema()
    {
        assertFails(MISSING_SCHEMA, "SHOW TABLES FROM NONEXISTENT_SCHEMA");
        assertFails(MISSING_SCHEMA, "SHOW TABLES IN NONEXISTENT_SCHEMA LIKE '%'");
    }

    @Test
    public void testNonAggregate()
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
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT f FROM t1");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 ORDER BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT count(*) FROM t1 GROUP BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 WHERE f > 1");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = "line 1:8: Column 't.y' cannot be resolved")
    public void testInvalidAttributeCorrectErrorMessage()
    {
        analyze("SELECT t.y FROM (VALUES 1) t(x)");
    }

    @Test
    public void testOrderByMustAppearInSelectWithDistinct()
    {
        assertFails(ORDER_BY_MUST_BE_IN_SELECT, "SELECT DISTINCT a FROM t1 ORDER BY b");
    }

    @Test
    public void testNonDeterministicOrderBy()
    {
        analyze("SELECT DISTINCT random() as b FROM t1 ORDER BY b");
        analyze("SELECT random() FROM t1 ORDER BY random()");
        analyze("SELECT a FROM t1 ORDER BY random()");
        assertFails(NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT, "SELECT DISTINCT random() FROM t1 ORDER BY random()");
    }

    @Test
    public void testNonBooleanWhereClause()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE a");
    }

    @Test
    public void testDistinctAggregations()
    {
        analyze("SELECT COUNT(DISTINCT a), SUM(a) FROM t1");
    }

    @Test
    public void testMultipleDistinctAggregations()
    {
        analyze("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
    {
        // TODO: analyze output
        analyze("SELECT a x FROM t1 ORDER BY x + 1");
        analyze("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1e0)");
        analyze("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY a.someField");
        analyze("SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(apply(1, x -> x))");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");

        assertFails(TYPE_MISMATCH, 3, 10,
                "SELECT x.c as x\n" +
                        "FROM (VALUES 1) x(c)\n" +
                        "ORDER BY x.c");
    }

    @Test
    public void testOrderByWithWildcard()
    {
        // TODO: validate output
        analyze("SELECT t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testOrderByWithGroupByAndSubquerySelectExpression()
    {
        analyze("SELECT a FROM t1 GROUP BY a ORDER BY (SELECT a)");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:46: Subquery uses 'b' which must appear in GROUP BY clause",
                "SELECT a FROM t1 GROUP BY a ORDER BY (SELECT b)");

        analyze("SELECT a AS b FROM t1 GROUP BY t1.a ORDER BY (SELECT b)");

        assertFails(
                REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                "line 2:22: Invalid reference to output projection attribute from ORDER BY aggregation",
                "SELECT a AS b FROM t1 GROUP BY t1.a \n" +
                        "ORDER BY MAX((SELECT b))");

        analyze("SELECT a FROM t1 GROUP BY a ORDER BY MAX((SELECT x FROM (VALUES 4) t(x)))");

        analyze("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                "FROM (VALUES (1, 2)) t(a, b)\n" +
                "GROUP BY b\n" +
                "ORDER BY (SELECT x.someField)");

        assertFails(
                REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                "line 4:22: Invalid reference to output projection attribute from ORDER BY aggregation",
                "SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                        "FROM (VALUES (1, 2)) t(a, b)\n" +
                        "GROUP BY b\n" +
                        "ORDER BY MAX((SELECT x.someField))");
    }

    @Test
    public void testTooManyGroupingElements()
    {
        Session session = testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig().setMaxGroupingSets(2048)))).build();
        analyze(session, "SELECT a, b, c, d, e, f, g, h, i, j, k, SUM(l)" +
                "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))\n" +
                "t (a, b, c, d, e, f, g, h, i, j, k, l)\n" +
                "GROUP BY CUBE (a, b, c, d, e, f), CUBE (g, h, i, j, k)");
        assertFails(session, TOO_MANY_GROUPING_SETS,
                "line 3:10: GROUP BY has 4096 grouping sets but can contain at most 2048",
                "SELECT a, b, c, d, e, f, g, h, i, j, k, l, SUM(m)" +
                        "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))\n" +
                        "t (a, b, c, d, e, f, g, h, i, j, k, l, m)\n" +
                        "GROUP BY CUBE (a, b, c, d, e, f), CUBE (g, h, i, j, k, l)");
        assertFails(session, TOO_MANY_GROUPING_SETS,
                format("line 3:10: GROUP BY has more than %s grouping sets but can contain at most 2048", Integer.MAX_VALUE),
                "SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                        "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae, SUM(af)" +
                        "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, " +
                        "17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32))\n" +
                        "t (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                        "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae, af)\n" +
                        "GROUP BY CUBE (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                        "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae)");
    }

    @Test
    public void testMismatchedColumnAliasCount()
    {
        assertFails(MISMATCHED_COLUMN_ALIASES, "SELECT * FROM t1 u (x, y)");
    }

    @Test
    public void testJoinOnConstantExpression()
    {
        analyze("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testJoinOnNonBooleanExpression()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 JOIN t2 ON 5");
    }

    @Test
    public void testJoinOnAmbiguousName()
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT * FROM t1 JOIN t2 ON a = a");
    }

    @Test
    public void testNonEquiOuterJoin()
    {
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 RIGHT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testNonBooleanHaving()
    {
        assertFails(TYPE_MISMATCH, "SELECT sum(a) FROM t1 HAVING sum(a)");
    }

    @Test
    public void testAmbiguousReferenceInOrderBy()
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, b x FROM t1 ORDER BY x");
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, a x FROM t1 ORDER BY x");
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a, a FROM t1 ORDER BY a");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        // TODO: validate output
        analyze("SELECT * FROM t1, t2");
    }

    @Test
    public void testNaturalJoinNotSupported()
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 NATURAL JOIN t2");
    }

    @Test
    public void testNestedWindowFunctions()
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

        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 5e-1 PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 'foo' PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5e-1 FOLLOWING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 'foo' FOLLOWING)");
    }

    @Test
    public void testDistinctInWindowFunctionParameter()
    {
        assertFails(NOT_SUPPORTED, "SELECT a, count(DISTINCT b) OVER () FROM t1");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testGroupByWithRowExpression()
    {
        // TODO: verify output
        analyze("SELECT (a, b) FROM t1 GROUP BY a, b");
    }

    @Test
    public void testHaving()
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }

    @Test
    public void testWithCaseInsensitiveResolution()
    {
        // TODO: verify output
        analyze("WITH AB AS (SELECT * FROM t1) SELECT * FROM ab");
    }

    @Test
    public void testStartTransaction()
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
    {
        analyze("COMMIT");
        analyze("COMMIT WORK");
    }

    @Test
    public void testRollback()
    {
        analyze("ROLLBACK");
        analyze("ROLLBACK WORK");
    }

    @Test
    public void testExplainAnalyze()
    {
        analyze("EXPLAIN ANALYZE SELECT * FROM t1");
    }

    @Test
    public void testInsert()
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
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     a AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testCaseInsensitiveDuplicateWithQuery()
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     A AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testWithForwardReference()
    {
        assertFails(MISSING_TABLE,
                "WITH a AS (SELECT * FROM b)," +
                        "     b AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testExpressions()
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

    @Test
    public void testLike()
    {
        analyze("SELECT '1' LIKE '1'");
        analyze("SELECT CAST('1' as CHAR(1)) LIKE '1'");
    }

    @Test(enabled = false) // TODO: need to support widening conversion for numbers
    public void testInWithNumericTypes()
    {
        analyze("SELECT * FROM t1 WHERE 1 IN (1, 2, 3.5)");
    }

    @Test
    public void testWildcardWithoutFrom()
    {
        assertFails(WILDCARD_WITHOUT_FROM, "SELECT *");
    }

    @Test
    public void testReferenceWithoutFrom()
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT dummy");
    }

    @Test
    public void testGroupBy()
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByEmpty()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a FROM t1 GROUP BY ()");
    }

    @Test
    public void testSingleGroupingSet()
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
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 1 not in GROUP BY clause", "SELECT * FROM (SELECT a + 1, b FROM t1) t GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 't.a' not in GROUP BY clause", "SELECT * FROM (SELECT a, b FROM t1) t GROUP BY b ORDER BY 1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 'a' not in GROUP BY clause", "SELECT * FROM (SELECT a, b FROM t1) GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "Column 1 not in GROUP BY clause", "SELECT * FROM (SELECT a + 1, b FROM t1) GROUP BY b ORDER BY 1");
    }

    @Test
    public void testGroupByCase()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE a WHEN 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN 0 ELSE a END, count(*) FROM t1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN a = 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN 0 ELSE a END, count(*) FROM t1");
    }

    @Test
    public void testGroupingWithWrongColumnsAndNoGroupBy()
    {
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT a, SUM(b), GROUPING(a, b, c, d) FROM t1 GROUP BY GROUPING SETS ((a, b), (c))");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT a, SUM(b), GROUPING(a, b) FROM t1");
    }

    @Test
    public void testMismatchedUnionQueries()
    {
        assertFails(TYPE_MISMATCH, "SELECT 1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "SELECT a FROM t1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "(SELECT 1) UNION SELECT 'a'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 1, 2 UNION SELECT 1");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 'a' UNION SELECT 'b', 'c'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "TABLE t2 UNION SELECT 'a'");
        assertFails(
                TYPE_MISMATCH,
                ".* column 1 in UNION query has incompatible types.*",
                "SELECT 123, 'foo' UNION ALL SELECT 'bar', 999");
        assertFails(
                TYPE_MISMATCH,
                ".* column 2 in UNION query has incompatible types.*",
                "SELECT 123, 123 UNION ALL SELECT 999, 'bar'");
    }

    @Test
    public void testUnionUnmatchedOrderByAttribute()
    {
        assertFails(MISSING_ATTRIBUTE, "TABLE t2 UNION ALL SELECT c, d FROM t1 ORDER BY c");
    }

    @Test
    public void testGroupByComplexExpressions()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NOT NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(CAST(a AS VARCHAR) LIKE 'a', 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a IN (1, 2, 3) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 1 IN (a, 2, 3) FROM t1 GROUP BY b");
    }

    @Test
    public void testNonNumericTableSamplePercentage()
    {
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI ('a')");
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (a + 1)");
    }

    @Test
    public void testTableSampleOutOfRange()
    {
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-1)");
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-101)");
    }

    @Test
    public void testCreateTableAsColumns()
    {
        // TODO: validate output
        analyze("CREATE TABLE test(a) AS SELECT 123");
        analyze("CREATE TABLE test(a, b) AS SELECT 1, 2");
        analyze("CREATE TABLE test(a) AS (VALUES 1)");

        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE TABLE test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE TABLE test AS SELECT 1 a, 2 a");
        assertFails(COLUMN_TYPE_UNKNOWN, "CREATE TABLE test AS SELECT null a");
        assertFails(MISMATCHED_COLUMN_ALIASES, 1, 19, "CREATE TABLE test(x) AS SELECT 1, 2");
        assertFails(MISMATCHED_COLUMN_ALIASES, 1, 19, "CREATE TABLE test(x, y) AS SELECT 1");
        assertFails(MISMATCHED_COLUMN_ALIASES, 1, 19, "CREATE TABLE test(x, y) AS (VALUES 1)");
        assertFails(DUPLICATE_COLUMN_NAME, 1, 24, "CREATE TABLE test(abc, AbC) AS SELECT 1, 2");
        assertFails(COLUMN_TYPE_UNKNOWN, 1, 1, "CREATE TABLE test(x) AS SELECT null");
        assertFails(MISSING_ATTRIBUTE, ".*Column 'y' cannot be resolved", "CREATE TABLE test(x) WITH (p1 = y) AS SELECT null");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test(x) WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3') AS SELECT null");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test(x) WITH (p1 = 'p1', \"p1\" = 'p2') AS SELECT null");
    }

    @Test
    public void testCreateTable()
    {
        analyze("CREATE TABLE test (id bigint)");
        analyze("CREATE TABLE test (id bigint) WITH (p1 = 'p1')");

        assertFails(MISSING_ATTRIBUTE, ".*Column 'y' cannot be resolved", "CREATE TABLE test (x bigint) WITH (p1 = y)");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test (id bigint) WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3')");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test (id bigint) WITH (p1 = 'p1', \"p1\" = 'p2')");
    }

    @Test
    public void testCreateSchema()
    {
        analyze("CREATE SCHEMA test");
        analyze("CREATE SCHEMA test WITH (p1 = 'p1')");

        assertFails(MISSING_ATTRIBUTE, ".*Column 'y' cannot be resolved", "CREATE SCHEMA test WITH (p1 = y)");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE SCHEMA test WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3')");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE SCHEMA test WITH (p1 = 'p1', \"p1\" = 'p2')");
    }

    @Test
    public void testCreateViewColumns()
    {
        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE VIEW test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE VIEW test AS SELECT 1 a, 2 a");
        assertFails(COLUMN_TYPE_UNKNOWN, "CREATE VIEW test AS SELECT null a");
    }

    @Test
    public void testCreateRecursiveView()
    {
        assertFails(VIEW_IS_RECURSIVE, "CREATE OR REPLACE VIEW v1 AS SELECT * FROM v1");
    }

    @Test
    public void testExistingRecursiveView()
    {
        analyze("SELECT * FROM v1 a JOIN v1 b ON a.a = b.a");
        analyze("SELECT * FROM v1 a JOIN (SELECT * from v1) b ON a.a = b.a");
        assertFails(VIEW_ANALYSIS_ERROR, "SELECT * FROM v5");
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
    {
        assertFails(VIEW_IS_STALE, "SELECT * FROM v2");
    }

    @Test
    public void testStoredViewAnalysisScoping()
    {
        // the view must not be analyzed using the query context
        analyze("WITH t1 AS (SELECT 123 x) SELECT * FROM v1");
    }

    @Test
    public void testStoredViewResolution()
    {
        // the view must be analyzed relative to its own catalog/schema
        analyze("SELECT * FROM c3.s3.v3");
    }

    @Test
    public void testQualifiedViewColumnResolution()
    {
        // it should be possible to qualify the column reference with the view name
        analyze("SELECT v1.a FROM v1");
    }

    @Test
    public void testViewWithUppercaseColumn()
    {
        analyze("SELECT * FROM v4");
    }

    @Test
    public void testUse()
    {
        assertFails(NOT_SUPPORTED, "USE foo");
    }

    @Test
    public void testNotNullInJoinClause()
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON a.x IS NOT NULL");
    }

    @Test
    public void testIfInJoinClause()
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON IF(a.x = 1, true, false)");
    }

    @Test
    public void testLiteral()
    {
        assertFails(INVALID_LITERAL, "SELECT TIMESTAMP '2012-10-31 01:00:00 PT'");
    }

    @Test
    public void testLambda()
    {
        analyze("SELECT apply(5, x -> abs(x)) from t1");
        assertFails(STANDALONE_LAMBDA, "SELECT x -> abs(x) from t1");
    }

    @Test
    public void testLambdaCapture()
    {
        analyze("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)");
        analyze("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)");

        // reference lambda variable of the not-immediately-enclosing lambda
        analyze("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)");
    }

    @Test
    public void testLambdaInAggregationContext()
    {
        analyze("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        analyze("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
        analyze("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
        analyze("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(sum(x), i -> i * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, y -> x) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, y -> x.someField) FROM (VALUES (CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(x,y) GROUP BY y");
        analyze("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        analyze("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // nested lambda expression uses the same variable name
        analyze("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // illegal use of a column whose name is the same as a lambda variable name
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(sum(x), x -> x * x) + x FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(sum(x), x -> apply(x, x -> x * x)) + x FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // x + y within lambda should not be treated as group expression
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, y -> x + y) FROM (VALUES (1,2)) t(x, y) GROUP BY x+y");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, x -> y + transform(array[1], z -> x)[1]) FROM (VALUES (1, 2)) t(x,y) GROUP BY y + transform(array[1], z -> x)[1]");
    }

    @Test
    public void testLambdaInSubqueryContext()
    {
        analyze("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)");
        analyze("SELECT apply((SELECT 10), i -> i * i)");

        // with capture
        analyze("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)");
        analyze("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)");
        analyze("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)");
    }

    @Test
    public void testLambdaWithAggregationAndGrouping()
    {
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT transform(ARRAY[1], y -> max(x)) FROM (VALUES 10) t(x)");

        // use of aggregation/window function on lambda variable
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT apply(1, x -> max(x)) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> max(x.someField)) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT apply(1, x -> grouping(x)) FROM (VALUES (1, 2)) t(x, y) GROUP BY y");
    }

    @Test
    public void testLambdaWithSubquery()
    {
        assertFails(
                NOT_SUPPORTED,
                ".* Lambda expression cannot contain subqueries",
                "SELECT apply(1, i -> (SELECT 3)) FROM (VALUES 1) t(x)");
        assertFails(
                NOT_SUPPORTED,
                ".* Lambda expression cannot contain subqueries",
                "SELECT apply(1, i -> (SELECT i)) FROM (VALUES 1) t(x)");

        // GROUP BY column captured in lambda
        analyze(
                "SELECT (SELECT apply(0, x -> x + b) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");

        // non-GROUP BY column captured in lambda
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:34: Subquery uses 'a' which must appear in GROUP BY clause",
                "SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(c)) " +
                        "FROM t1 u GROUP BY b");
        // TODO #7784
//        assertFails(
//                MUST_BE_AGGREGATE_OR_GROUP_BY,
//                "line 1:34: Subquery uses '\"u.a\"' which must appear in GROUP BY clause",
//                "SELECT (SELECT apply(0, x -> x + u.a) from (values 1) x(a)) " +
//                        "FROM t1 u GROUP BY b");

        // name shadowing
        analyze("SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");
        analyze("SELECT (SELECT apply(0, a -> a + a)) FROM t1 u GROUP BY b");
    }

    @Test
    public void testLambdaWithSubqueryInOrderBy()
    {
        analyze("SELECT a FROM t1 ORDER BY (SELECT apply(0, x -> x + a))");
        analyze("SELECT a AS output_column FROM t1 ORDER BY (SELECT apply(0, x -> x + output_column))");
        analyze("SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + a))");
        analyze("SELECT count(*) AS output_column FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + output_column))");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:71: Subquery uses 'b' which must appear in GROUP BY clause",
                "SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + b))");
    }

    @Test
    public void testLambdaWithInvalidParameterCount()
    {
        assertFails(INVALID_PARAMETER_USAGE, "line 1:17: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT apply(5, (x, y) -> 6)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:17: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT apply(5, (x, y, z) -> 6)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:21: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT TRY(apply(5, (x, y) -> x + 1) / 0)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:21: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT TRY(apply(5, (x, y, z) -> x + 1) / 0)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:29: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT filter(ARRAY [5, 6], (x, y) -> x = 5)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:29: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT filter(ARRAY [5, 6], (x, y, z) -> x = 5)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT map_filter(map(ARRAY [5, 6], ARRAY [5, 6]), (x) -> x = 1)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT map_filter(map(ARRAY [5, 6], ARRAY [5, 6]), (x, y, z) -> x = y + z)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:33: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT reduce(ARRAY [5, 20], 0, (s) -> s, s -> s)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:33: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT reduce(ARRAY [5, 20], 0, (s, x, z) -> s + x, s -> s + z)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:32: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT transform(ARRAY [5, 6], (x, y) -> x + y)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:32: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT transform(ARRAY [5, 6], (x, y, z) -> x + y + z)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:49: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT transform_keys(map(ARRAY[1], ARRAY [2]), k -> k)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT transform_keys(MAP(ARRAY['a'], ARRAY['b']), (k, v, x) -> k + 1)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:51: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT transform_values(map(ARRAY[1], ARRAY [2]), k -> k)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:51: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT transform_values(map(ARRAY[1], ARRAY [2]), (k, v, x) -> k + 1)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:39: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT zip_with(ARRAY[1], ARRAY['a'], x -> x)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:39: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT zip_with(ARRAY[1], ARRAY['a'], (x, y, z) -> (x, y, z))");
    }

    @Test
    public void testInvalidDelete()
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
    {
        assertFails(TYPE_MISMATCH, "SELECT 'abc' AT TIME ZONE 'America/Los_Angeles'");
    }

    @Test
    public void testValidJoinOnClause()
    {
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON TRUE");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1=1");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x=b.x AND a.y=b.y");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON NULL");
    }

    @Test
    public void testInValidJoinOnClause()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x + b.x");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON ROW (TRUE)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON (a.x=b.x, a.y=b.y)");
    }

    @Test
    public void testInvalidAggregationFilter()
    {
        assertFails(NOT_SUPPORTED, "SELECT sum(x) FILTER (WHERE x > 1) OVER (PARTITION BY x) FROM (VALUES (1), (2), (2), (4)) t (x)");
        assertFails(MUST_BE_AGGREGATION_FUNCTION, "SELECT abs(x) FILTER (where y = 1) FROM (VALUES (1, 1)) t(x, y)");
        assertFails(MUST_BE_AGGREGATION_FUNCTION, "SELECT abs(x) FILTER (where y = 1) FROM (VALUES (1, 1, 1)) t(x, y, z) GROUP BY z");
    }

    @Test
    void testAggregationWithOrderBy()
    {
        analyze("SELECT array_agg(DISTINCT x ORDER BY x) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        analyze("SELECT array_agg(x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails(ORDER_BY_MUST_BE_IN_AGGREGATE, "SELECT array_agg(DISTINCT x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails(MUST_BE_AGGREGATION_FUNCTION, "SELECT abs(x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails(TYPE_MISMATCH, "SELECT array_agg(x ORDER BY x) FROM (VALUES MAP(ARRAY['a'], ARRAY['b'])) t(x)");
        assertFails(MISSING_ATTRIBUTE, "SELECT 1 as a, array_agg(x ORDER BY a) FROM (VALUES (1), (2), (3)) t(x)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT 1 AS c FROM (VALUES (1), (2)) t(x) ORDER BY sum(x order by c)");
    }

    @Test
    public void testQuantifiedComparisonExpression()
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

    @Test
    public void testJoinUnnest()
    {
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN UNNEST(x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true");
    }

    @Test
    public void testJoinLateral()
    {
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN LATERAL(VALUES x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN LATERAL(VALUES x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN LATERAL(VALUES x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN LATERAL(VALUES x) ON true");
    }

    @BeforeClass
    public void setup()
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
                        new ColumnMetadata("d", BIGINT))),
                false));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table2,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT))),
                false));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table3,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("x", BIGINT, null, true))),
                false));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        inSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG, new ConnectorTableMetadata(table4,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT))),
                false));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table5,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT, null, true))),
                false));

        // table with a varchar column
        SchemaTableName table6 = new SchemaTableName("s1", "t6");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table6,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", VARCHAR),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT))),
                false));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table7 = new SchemaTableName("s1", "t7");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table7,
                ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", DOUBLE),
                        new ColumnMetadata("c", new ArrayType(BIGINT)),
                        new ColumnMetadata("d", new ArrayType(DOUBLE)))),
                false));

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

        // recursive view referencing to itself
        String viewData5 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select * from v5",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v5"), viewData5, false));
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

    private void assertFails(SemanticErrorCode error, int line, int column, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, Optional.of(new NodeLocation(line, column - 1)), query);
    }

    private void assertFails(SemanticErrorCode error, String message, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, message, query);
    }

    private void assertFails(Session session, SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(session, error, Optional.empty(), query);
    }

    private void assertFails(Session session, SemanticErrorCode error, Optional<NodeLocation> location, @Language("SQL") String query)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }

            if (location.isPresent()) {
                NodeLocation expected = location.get();
                NodeLocation actual = e.getNode().getLocation().get();

                if (expected.getLineNumber() != actual.getLineNumber() || expected.getColumnNumber() != actual.getColumnNumber()) {
                    fail(format(
                            "Expected error '%s' to occur at line %s, offset %s, but was: line %s, offset %s",
                            e.getCode(),
                            expected.getLineNumber(),
                            expected.getColumnNumber(),
                            actual.getLineNumber(),
                            actual.getColumnNumber()));
                }
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

            if (!e.getMessage().matches(message)) {
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
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
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
