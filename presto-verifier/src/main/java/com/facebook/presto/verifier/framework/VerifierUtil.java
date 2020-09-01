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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class VerifierUtil
{
    private VerifierUtil() {}

    public static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();

    public static Identifier delimitedIdentifier(String name)
    {
        return new Identifier(name, true);
    }

    public static void runAndConsume(Callable<QueryActionStats> callable, Consumer<QueryActionStats> queryStatsConsumer)
    {
        runAndConsume(callable, queryStatsConsumer, e -> {});
    }

    public static void runAndConsume(Callable<QueryActionStats> callable, Consumer<QueryActionStats> queryStatsConsumer, Consumer<QueryException> queryExceptionConsumer)
    {
        callAndConsume(callable, identity(), queryStatsConsumer, queryExceptionConsumer);
    }

    public static <V> QueryResult<V> callAndConsume(Callable<QueryResult<V>> callable, Consumer<QueryActionStats> queryStatsConsumer)
    {
        return callAndConsume(callable, queryStatsConsumer, e -> {});
    }

    public static <V> QueryResult<V> callAndConsume(Callable<QueryResult<V>> callable, Consumer<QueryActionStats> queryStatsConsumer, Consumer<QueryException> queryExceptionConsumer)
    {
        return callAndConsume(callable, QueryResult::getQueryActionStats, queryStatsConsumer, queryExceptionConsumer);
    }

    private static <V> V callAndConsume(
            Callable<V> callable,
            Function<V, QueryActionStats> queryStatsTransformer,
            Consumer<QueryActionStats> queryStatsConsumer,
            Consumer<QueryException> queryExceptionConsumer)
    {
        try {
            V result = callable.call();
            queryStatsConsumer.accept(queryStatsTransformer.apply(result));
            return result;
        }
        catch (PrestoQueryException e) {
            queryStatsConsumer.accept(e.getQueryActionStats());
            queryExceptionConsumer.accept(e);
            throw e;
        }
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

    public static List<Type> getColumnTypes(TypeManager typeManager, ResultSetMetaData metadata)
    {
        return callUnchecked(() ->
                IntStream.rangeClosed(1, metadata.getColumnCount())
                        .mapToObj(i -> callUnchecked(() -> metadata.getColumnTypeName(i)))
                        .map(TypeSignature::parseTypeSignature)
                        .map(typeManager::getType)
                        .collect(toImmutableList()));
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
