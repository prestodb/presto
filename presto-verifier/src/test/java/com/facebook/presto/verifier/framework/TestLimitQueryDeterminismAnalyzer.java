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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.TestingResultSetMetaData;
import com.facebook.presto.verifier.TestingResultSetMetaData.ColumnInfo;
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_LIMIT_CLAUSE;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NON_DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NOT_RUN;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestLimitQueryDeterminismAnalyzer
{
    private static class MockPrestoAction
            implements PrestoAction
    {
        private final List<Object> rows;
        private final ResultSetMetaData metadata;
        private Statement lastStatement;

        public MockPrestoAction(List<?> rows, List<ColumnInfo> columns)
        {
            this.rows = ImmutableList.copyOf(rows);
            this.metadata = new TestingResultSetMetaData(columns);
        }

        @Override
        public QueryActionStats execute(Statement statement, QueryStage queryStage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, ResultSetConverter<R> converter)
        {
            lastStatement = statement;
            return new QueryResult(rows, metadata, QUERY_STATS);
        }

        public Statement getLastStatement()
        {
            return lastStatement;
        }
    }

    private static final long ROW_COUNT_WITH_LIMIT = 1000;
    private static final QueryActionStats QUERY_STATS = new QueryActionStats(
            Optional.of(new QueryStats("id", "", false, false, false, 1, 2, 3, 4, 5, 0, 7, 8, 9, 10, 11, 12, 0, 0, 0, Optional.empty())),
            Optional.empty());
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private static final String ORDER_BY_LIMIT_QUERY = "INSERT INTO test\n" +
            "SELECT\n" +
            "    a b,\n" +
            "    c,\n" +
            "    *,\n" +
            "    e\n" +
            "FROM source\n" +
            "ORDER BY\n" +
            "    a,\n" +
            "    b,\n" +
            "    2 DESC,\n" +
            "    c + d DESC\n" +
            "LIMIT\n" +
            "    1000";
    private static final List<ColumnInfo> TIE_INSPECTOR_COLUMNS = ImmutableList.of(
            new ColumnInfo("b", INTEGER),
            new ColumnInfo("c", INTEGER),
            new ColumnInfo("e", INTEGER),
            new ColumnInfo("x", INTEGER),
            new ColumnInfo("y", INTEGER),
            new ColumnInfo("$$sort_key$$0", INTEGER),
            new ColumnInfo("$$sort_key$$3", INTEGER));

    @Test
    public void testNotRunLimitNoOrderBy()
    {
        MockPrestoAction prestoAction = createPrestoAction(1000);

        // Unsupported statement types
        assertAnalysis(prestoAction, "CREATE TABLE test (x varchar, ds varhcar) WITH (partitioned_by = ARRAY[\"ds\"])", NOT_RUN);
        assertAnalysis(prestoAction, "SELECT * FROM source LIMIT 10", NOT_RUN);

        // ORDER BY clause
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source UNION ALL SELECT * FROM source ORDER BY 1 LIMIT 1000", NOT_RUN);

        // No outer LIMIT clause
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source UNION ALL SELECT * FROM source", NOT_RUN);
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source", NOT_RUN);
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM (SELECT * FROM source LIMIT 1000)", NOT_RUN);

        // LIMIT ALL clause
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source LIMIT all", NOT_RUN);
        assertAnalysis(prestoAction, "CREATE TABLE test AS (WITH f AS (select * from g) ((SELECT * FROM source UNION ALL SELECT * FROM source LIMIT ALL)))", NOT_RUN);
    }

    @Test
    public void testNonDeterministicLimitNoOrderBy()
    {
        MockPrestoAction prestoAction = createPrestoAction(1001);

        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source LIMIT 1000", NON_DETERMINISTIC);
        assertAnalyzerQuery(prestoAction, "SELECT count(1) FROM (SELECT * FROM source LIMIT 1001)");

        assertAnalysis(prestoAction, "CREATE TABLE test AS (WITH f AS (select * from g) ((SELECT * FROM source UNION ALL SELECT * FROM source LIMIT 1000)))", NON_DETERMINISTIC);
        assertAnalyzerQuery(prestoAction, "SELECT count(1) FROM (WITH f AS (select * from g) SELECT * FROM source UNION ALL SELECT * FROM source LIMIT 1001)");

        assertAnalysis(prestoAction, "CREATE TABLE test AS (WITH f AS (select * from g) (SELECT * FROM source LIMIT 1000))", NON_DETERMINISTIC);
        assertAnalyzerQuery(prestoAction, "SELECT count(1) FROM (WITH f AS (select * from g) SELECT * FROM source LIMIT 1001)");
    }

    @Test
    public void testDeterministicLimitNoOrderBy()
    {
        MockPrestoAction prestoAction = createPrestoAction(1000);
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source LIMIT 1000", 500, DETERMINISTIC, false);
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source UNION ALL SELECT * FROM source LIMIT 1000", 500, DETERMINISTIC, false);
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source LIMIT 1000", DETERMINISTIC);
    }

    @Test
    public void testFailedDataChangedLimitNoOrderBy()
    {
        assertAnalysis(createPrestoAction(999), "INSERT INTO test SELECT * FROM source LIMIT 1000", FAILED_DATA_CHANGED);
    }

    @Test
    public void testLimitOrderByNonDeterministic()
    {
        MockPrestoAction prestoAction = createPrestoAction(
                ImmutableList.of(
                        ImmutableList.of(1, 2, 3, 4, 5, 1, 6),
                        ImmutableList.of(1, 2, 0, 0, 0, 1, 6)),
                TIE_INSPECTOR_COLUMNS);
        assertAnalysis(prestoAction, ORDER_BY_LIMIT_QUERY, NON_DETERMINISTIC);
        assertAnalyzerQuery(
                prestoAction,
                "SELECT\n" +
                        "  a b\n" +
                        ", c\n" +
                        ", *\n" +
                        ", e\n" +
                        ", a \"$$sort_key$$0\"\n" +
                        ", (c + d) \"$$sort_key$$3\"\n" +
                        "FROM\n" +
                        "  source\n" +
                        "ORDER BY a ASC, b ASC, 2 DESC, (c + d) DESC\n" +
                        "LIMIT 1001");
    }

    @Test
    public void testLimitOrderByDeterministic()
    {
        MockPrestoAction prestoAction = createPrestoAction(1000);
        assertAnalysis(prestoAction, "INSERT INTO test SELECT * FROM source ORDER BY 1 LIMIT 1000", 500, DETERMINISTIC, false);

        prestoAction = createPrestoAction(ImmutableList.of(ImmutableList.of(1, 2, 3, 4, 5, 1, 6)), TIE_INSPECTOR_COLUMNS);
        assertAnalysis(prestoAction, ORDER_BY_LIMIT_QUERY, DETERMINISTIC);

        prestoAction = createPrestoAction(
                ImmutableList.of(
                        ImmutableList.of(1, 2, 3, 4, 5, 1, 6),
                        ImmutableList.of(1, 2, 0, 0, 0, 1, 5)),
                TIE_INSPECTOR_COLUMNS);
        assertAnalysis(prestoAction, ORDER_BY_LIMIT_QUERY, DETERMINISTIC);
    }

    @Test
    public void testLimitOrderByFailedDataChanged()
    {
        MockPrestoAction prestoAction = createPrestoAction(ImmutableList.of(), TIE_INSPECTOR_COLUMNS);
        assertAnalysis(prestoAction, ORDER_BY_LIMIT_QUERY, FAILED_DATA_CHANGED);
    }

    private static MockPrestoAction createPrestoAction(long rowCount)
    {
        return new MockPrestoAction(ImmutableList.of(rowCount), ImmutableList.of());
    }

    private static MockPrestoAction createPrestoAction(List<List<Object>> rows, List<ColumnInfo> columns)
    {
        return new MockPrestoAction(rows, columns);
    }

    private static void assertAnalysis(PrestoAction prestoAction, String query, LimitQueryDeterminismAnalysis expectedAnalysis)
    {
        assertAnalysis(prestoAction, query, ROW_COUNT_WITH_LIMIT, expectedAnalysis, expectedAnalysis != NOT_RUN);
    }

    private static void assertAnalysis(PrestoAction prestoAction, String query, long controlRowCount, LimitQueryDeterminismAnalysis expectedAnalysis, boolean queryRan)
    {
        DeterminismAnalysisDetails.Builder determinismAnalysisDetailsBuilder = DeterminismAnalysisDetails.builder();
        LimitQueryDeterminismAnalysis analysis = new LimitQueryDeterminismAnalyzer(
                prestoAction,
                true,
                sqlParser.createStatement(query, PARSING_OPTIONS),
                controlRowCount,
                determinismAnalysisDetailsBuilder).analyze();
        DeterminismAnalysisDetails determinismAnalysisDetails = determinismAnalysisDetailsBuilder.build(NON_DETERMINISTIC_LIMIT_CLAUSE);

        assertEquals(analysis, expectedAnalysis);
        assertEquals(determinismAnalysisDetails.getLimitQueryAnalysisQueryId() != null, queryRan);
    }

    private static void assertAnalyzerQuery(MockPrestoAction prestoAction, String expectedQuery)
    {
        Statement expectedStatement = sqlParser.createStatement(expectedQuery, PARSING_OPTIONS);
        Statement actualStatement = prestoAction.getLastStatement();
        assertEquals(formatSql(actualStatement, Optional.empty()), formatSql(expectedStatement, Optional.empty()));
    }
}
