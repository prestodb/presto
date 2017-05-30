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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.raptorx.metadata.CorruptMetadataException;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.postgresql.PGStatement;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_METADATA_ERROR;
import static com.google.common.collect.Iterables.partition;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DatabaseUtil
{
    static {
        requireParameterNames();
    }

    private static final Logger log = Logger.get(DatabaseUtil.class);

    private DatabaseUtil() {}

    public static Jdbi createJdbi(ConnectionFactory connectionFactory)
    {
        return Jdbi.create(connectionFactory)
                .installPlugin(new SqlObjectPlugin())
                .setSqlLogger(new SqlLogger()
                {
                    @Override
                    public void logAfterExecution(StatementContext context)
                    {
                        log.debug("Executed SQL in %s: %s",
                                succinctNanos(context.getElapsedTime(ChronoUnit.NANOS)),
                                context.getRenderedSql().replaceAll("\\s+", " "));
                    }
                });
    }

    public static PrestoException metadataError(Throwable cause)
    {
        return new PrestoException(RAPTOR_METADATA_ERROR, "Failed to perform metadata operation", cause);
    }

    public static void enableStreamingResults(Statement statement)
            throws SQLException
    {
        if (statement.isWrapperFor(com.mysql.jdbc.Statement.class)) {
            statement.unwrap(com.mysql.jdbc.Statement.class).enableStreamingResults();
        }
        else if (statement.isWrapperFor(PGStatement.class)) {
            statement.getConnection().setAutoCommit(false);
            statement.setFetchSize(1000);
        }
    }

    public static OptionalInt getOptionalInt(ResultSet rs, String name)
            throws SQLException
    {
        int value = rs.getInt(name);
        return rs.wasNull() ? OptionalInt.empty() : OptionalInt.of(value);
    }

    public static OptionalLong getOptionalLong(ResultSet rs, String name)
            throws SQLException
    {
        long value = rs.getLong(name);
        return rs.wasNull() ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public static String utf8String(byte[] bytes)
    {
        return (bytes == null) ? null : new String(bytes, UTF_8);
    }

    public static Optional<String> optionalUtf8String(byte[] bytes)
    {
        return Optional.ofNullable(bytes).map(DatabaseUtil::utf8String);
    }

    public static byte[] utf8Bytes(Optional<String> value)
    {
        return value.map(s -> s.getBytes(UTF_8)).orElse(null);
    }

    public static Integer boxedInt(OptionalInt value)
    {
        return value.isPresent() ? value.getAsInt() : null;
    }

    public static Long boxedLong(OptionalLong value)
    {
        return value.isPresent() ? value.getAsLong() : null;
    }

    @SuppressWarnings("Guava")
    public static <T> Iterable<Set<T>> partitionSet(Set<T> iterable, int size)
    {
        return FluentIterable.from(partition(iterable, size))
                .transform(ImmutableSet::copyOf);
    }

    public static void verifyMetadata(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new CorruptMetadataException(format(formatString, args));
        }
    }

    public static void requireParameterNames()
    {
        // we depend on parameter names being present for Jdbi SQL Object named parameters
        try {
            Method method = DatabaseUtil.class.getMethod("createJdbi", ConnectionFactory.class);
            if (!method.getParameters()[0].isNamePresent()) {
                throw new AssertionError("Parameter names are not present. Compile with -parameters");
            }
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }
}
