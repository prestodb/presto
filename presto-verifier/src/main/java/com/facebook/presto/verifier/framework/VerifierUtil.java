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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableMap;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.framework.AbstractVerification.formatSql;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM;
import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;

public class VerifierUtil
{
    private VerifierUtil() {}

    public static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();

    public static Identifier delimitedIdentifier(String name)
    {
        return new Identifier(name, true);
    }

    public static void runAndConsume(Callable<QueryStats> callable, Consumer<QueryStats> queryStatsConsumer)
    {
        runAndConsume(callable, queryStatsConsumer, e -> {});
    }

    public static void runAndConsume(Callable<QueryStats> callable, Consumer<QueryStats> queryStatsConsumer, Consumer<QueryException> queryExceptionConsumer)
    {
        callAndConsume(callable, identity(), queryStatsConsumer, queryExceptionConsumer);
    }

    public static <V> QueryResult<V> callAndConsume(Callable<QueryResult<V>> callable, Consumer<QueryStats> queryStatsConsumer)
    {
        return callAndConsume(callable, QueryResult::getQueryStats, queryStatsConsumer, e -> {});
    }

    private static <V> V callAndConsume(
            Callable<V> callable,
            Function<V, QueryStats> queryStatsTransformer,
            Consumer<QueryStats> queryStatsConsumer,
            Consumer<QueryException> queryExceptionConsumer)
    {
        try {
            V result = callable.call();
            queryStatsConsumer.accept(queryStatsTransformer.apply(result));
            return result;
        }
        catch (PrestoQueryException e) {
            e.getQueryStats().ifPresent(queryStatsConsumer);
            queryExceptionConsumer.accept(e);
            throw e;
        }
    }

    public static QueryResult<ChecksumResult> validateChecksum(
            PrestoAction prestoAction,
            ChecksumValidator checksumValidator,
            Statement controlChecksumQuery,
            Statement testChecksumQuery,
            ChecksumQueryContext controlContext,
            ChecksumQueryContext testContext)
    {
        controlContext.setChecksumQuery(formatSql(controlChecksumQuery));
        testContext.setChecksumQuery(formatSql(testChecksumQuery));

        QueryResult<ChecksumResult> controlChecksum = callAndConsume(
                () -> prestoAction.execute(controlChecksumQuery, CONTROL_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> controlContext.setChecksumQueryId(stats.getQueryId()));
        QueryResult<ChecksumResult> testChecksum = callAndConsume(
                () -> prestoAction.execute(testChecksumQuery, TEST_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> testContext.setChecksumQueryId(stats.getQueryId()));

        return match(checksumValidator, controlColumns, testColumns, getOnlyElement(controlChecksum.getResults()), getOnlyElement(testChecksum.getResults()));
    }

    public static List<String> getColumnNames(ResultSetMetaData metadata)
    {
        return callUnchecked(() ->
                IntStream.rangeClosed(1, metadata.getColumnCount())
                        .mapToObj(i -> callUnchecked(() -> metadata.getColumnName(i)))
                        .collect(toImmutableList()));
    }

    public static Map<String, Integer> getColumnIndices(ResultSetMetaData metadata)
    {
        return callUnchecked(() ->
                IntStream.rangeClosed(1, metadata.getColumnCount())
                        .boxed()
                        .collect(toImmutableMap(i -> callUnchecked(() -> metadata.getColumnName(i)), i -> i - 1)));
    }

    public static List<Column> getColumns(TypeManager typeManager, ResultSetMetaData metadata)
    {
        return callUnchecked(() ->
                IntStream.rangeClosed(1, metadata.getColumnCount())
                        .mapToObj(i -> callUnchecked(() -> Column.create(
                                metadata.getColumnName(i),
                                delimitedIdentifier(metadata.getColumnName(i)),
                                typeManager.getType(parseTypeSignature(metadata.getColumnTypeName(i))))))
                        .collect(toImmutableList()));
    }

    public static MatchResult matchSingleRowResults(QueryResult<List<Object>> controlResults, QueryResult<List<Object>> testResults, TypeManager typeManager)
    {
        List<Column> columns = getColumns(typeManager, controlResults.getMetadata());
        List<Column> testColumns = getColumns(typeManager, testResults.getMetadata());
        if (!Objects.equals(columns, testColumns)) {
            return new MatchResult(MatchResult.MatchType.SCHEMA_MISMATCH, Optional.empty(), OptionalLong.empty(), OptionalLong.empty(), ImmutableMap.of());
        }

        List<Object> controlResult = getOnlyElement(controlResults.getResults());
        List<Object> testResult = getOnlyElement(testResults.getResults());

        IntStream.range(0, columns.size())
                .boxed()
                .filter(i -> !Objects.equals(controlResult.get(i), testResult.get(i)))
                .collect(toImmutableMap(columns::get, i -> new ColumnMatchResult(false, columns.get(i), )))

        return new MatchResult()
    }

    private static <V> V callUnchecked(SqlExceptionCallable<V> callable)
    {
        try {
            return callable.call();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public interface Callable<V>
    {
        V call();
    }

    public interface SqlExceptionCallable<V>
    {
        V call()
                throws SQLException;
    }
}
