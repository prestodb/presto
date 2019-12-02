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
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NON_DETERMINISTIC;
import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NOT_RUN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestLimitQueryDeterminismAnalyzer
{
    private static class MockPrestoAction
            implements PrestoAction
    {
        private final AtomicLong rowCount;
        private Statement lastStatement;

        public MockPrestoAction(AtomicLong rowCount)
        {
            this.rowCount = requireNonNull(rowCount, "rowCount is null");
        }

        @Override
        public QueryStats execute(Statement statement, QueryStage queryStage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, ResultSetConverter<R> converter)
        {
            lastStatement = statement;
            return new QueryResult(ImmutableList.of(rowCount.get()), ImmutableList.of("_col0"), QUERY_STATS);
        }

        public Statement getLastStatement()
        {
            return lastStatement;
        }
    }

    private static final QualifiedName TABLE_NAME = QualifiedName.of("test");
    private static final long ROW_COUNT_WITH_LIMIT = 1000;
    private static final QueryStats QUERY_STATS = new QueryStats("id", "", false, false, 1, 2, 3, 4, 5, 0, 7, 8, 9, 10, 11, 0, Optional.empty());
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private AtomicLong rowCount = new AtomicLong();
    private MockPrestoAction prestoAction;
    private LimitQueryDeterminismAnalyzer analyzer;

    @BeforeMethod
    public void setup()
    {
        this.prestoAction = new MockPrestoAction(rowCount);
        this.analyzer = new LimitQueryDeterminismAnalyzer(prestoAction, new VerifierConfig());
    }

    @Test
    public void testNotRun()
    {
        // Unsupported statement types
        assertEquals(analyze("CREATE TABLE test (x varchar, ds varhcar) WITH (partitioned_by = ARRAY[\"ds\"])"), NOT_RUN);
        assertEquals(analyze("SELECT * FROM source LIMIT 10"), NOT_RUN);

        // Order by clause
        assertEquals(analyze("INSERT INTO test SELECT * FROM source UNION ALL SELECT * FROM source ORDER BY 1 LIMIT 1000"), NOT_RUN);
        assertEquals(analyze("INSERT INTO test SELECT * FROM source ORDER BY 1 LIMIT 1000"), NOT_RUN);

        // not outer limit clause
        assertEquals(analyze("INSERT INTO test SELECT * FROM source UNION ALL SELECT * FROM source"), NOT_RUN);
        assertEquals(analyze("INSERT INTO test SELECT * FROM source"), NOT_RUN);
        assertEquals(analyze("INSERT INTO test SELECT * FROM (SELECT * FROM source LIMIT 1000)"), NOT_RUN);
    }

    @Test
    public void testNonDeterministic()
    {
        rowCount.set(1001);
        assertEquals(analyze("INSERT INTO test SELECT * FROM source LIMIT 1000"), NON_DETERMINISTIC);
        assertRowCountQuery("SELECT count(1) FROM (SELECT * FROM source)");

        assertEquals(analyze("CREATE TABLE test AS (WITH f AS (select * from g) ((SELECT * FROM source UNION ALL SELECT * FROM source LIMIT 1000)))"), NON_DETERMINISTIC);
        assertRowCountQuery("SELECT count(1) FROM (WITH f AS (select * from g) SELECT * FROM source UNION ALL SELECT * FROM source)");

        assertEquals(analyze("CREATE TABLE test AS (WITH f AS (select * from g) (SELECT * FROM source LIMIT 1000))"), NON_DETERMINISTIC);
        assertRowCountQuery("SELECT count(1) FROM (WITH f AS (select * from g) SELECT * FROM source)");
    }

    @Test
    public void testDeterministic()
    {
        rowCount.set(1000);
        assertEquals(analyze("INSERT INTO test SELECT * FROM source LIMIT 1000"), DETERMINISTIC);
    }

    @Test
    public void testFailedDataChanged()
    {
        rowCount.set(999);
        assertEquals(analyze("INSERT INTO test SELECT * FROM source LIMIT 1000"), FAILED_DATA_CHANGED);
    }

    private LimitQueryDeterminismAnalysis analyze(String query)
    {
        return analyzer.analyze(
                new QueryBundle(
                        TABLE_NAME,
                        ImmutableList.of(),
                        sqlParser.createStatement(query, PARSING_OPTIONS),
                        ImmutableList.of(),
                        CONTROL),
                ROW_COUNT_WITH_LIMIT);
    }

    private void assertRowCountQuery(String expectedQuery)
    {
        Statement expectedStatement = sqlParser.createStatement(expectedQuery, PARSING_OPTIONS);
        Statement actualStatement = prestoAction.getLastStatement();
        String actualQuery = formatSql(actualStatement, Optional.empty());
        assertEquals(actualStatement, expectedStatement, format("expected:\n[%s]\nbut found:\n[%s]", formatSql(expectedStatement, Optional.empty()), actualQuery));
    }
}
