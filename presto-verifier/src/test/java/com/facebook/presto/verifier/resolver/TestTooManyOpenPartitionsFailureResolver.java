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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.sql.parser.IdentifierSymbol;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.TestingResultSetMetaData;
import com.facebook.presto.verifier.TestingResultSetMetaData.ColumnInfo;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.facebook.presto.verifier.framework.QueryResult;
import com.facebook.presto.verifier.framework.QueryStage;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestTooManyOpenPartitionsFailureResolver
        extends AbstractTestPrestoQueryFailureResolver
{
    private static class MockPrestoAction
            implements PrestoAction
    {
        private final AtomicReference<String> createTable;

        public MockPrestoAction(AtomicReference<String> createTable)
        {
            this.createTable = requireNonNull(createTable, "createTable is null");
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
            return new QueryResult(
                    ImmutableList.of(createTable.get()),
                    new TestingResultSetMetaData(ImmutableList.of(new ColumnInfo("Create Table", VARCHAR))),
                    createQueryActionStats(0, 0));
        }
    }

    private static final String TABLE_NAME = "test";
    private static final int MAX_BUCKETS_PER_WRITER = 100;
    private static final QueryObjectBundle TEST_BUNDLE = new QueryObjectBundle(
            QualifiedName.of(TABLE_NAME),
            ImmutableList.of(),
            new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN, COLON)).createStatement(
                    "INSERT INTO test SELECT * FROM source",
                    ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build()),
            ImmutableList.of(),
            TEST);
    private static final QueryException HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION = new PrestoQueryException(
            new RuntimeException(),
            false,
            TEST_MAIN,
            Optional.of(HIVE_TOO_MANY_OPEN_PARTITIONS),
            createQueryActionStats(0, 0));

    private static final AtomicReference<String> createTable = new AtomicReference<>();

    public TestTooManyOpenPartitionsFailureResolver()
    {
        super(new TooManyOpenPartitionsFailureResolver(
                new SqlParser(new SqlParserOptions().allowIdentifierSymbol(IdentifierSymbol.AT_SIGN, COLON)),
                new MockPrestoAction(createTable),
                Suppliers.ofInstance(1),
                MAX_BUCKETS_PER_WRITER));
    }

    @Test
    public void testUnresolvedSufficientWorker()
    {
        createTable.set(format("CREATE TABLE %s (x varchar, ds varchar) WITH (partitioned_by = ARRAY[\"ds\"], bucket_count = 100)", TABLE_NAME));
        getFailureResolver().resolveQueryFailure(CONTROL_QUERY_STATS, HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION, Optional.of(TEST_BUNDLE));
        assertFalse(getFailureResolver().resolveQueryFailure(CONTROL_QUERY_STATS, HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION, Optional.of(TEST_BUNDLE)).isPresent());
    }

    @Test
    public void testUnresolvedNonBucketed()
    {
        createTable.set(format("CREATE TABLE %s (x varchar, ds varchar) WITH (partitioned_by = ARRAY[\"ds\"])", TABLE_NAME));
        getFailureResolver().resolveQueryFailure(CONTROL_QUERY_STATS, HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION, Optional.of(TEST_BUNDLE));
        assertFalse(getFailureResolver().resolveQueryFailure(CONTROL_QUERY_STATS, HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION, Optional.of(TEST_BUNDLE)).isPresent());
    }

    @Test
    public void testResolved()
    {
        createTable.set(format("CREATE TABLE %s (x varchar, ds varchar) WITH (partitioned_by = ARRAY[\"ds\"], bucket_count = 101)", TABLE_NAME));
        getFailureResolver().resolveQueryFailure(CONTROL_QUERY_STATS, HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION, Optional.of(TEST_BUNDLE));
        assertEquals(
                getFailureResolver().resolveQueryFailure(CONTROL_QUERY_STATS, HIVE_TOO_MANY_OPEN_PARTITIONS_EXCEPTION, Optional.of(TEST_BUNDLE)),
                Optional.of("Not enough workers on test cluster"));
    }
}
