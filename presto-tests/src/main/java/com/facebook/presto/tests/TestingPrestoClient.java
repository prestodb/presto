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

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestamp;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;

public class TestingPrestoClient
        implements Closeable
{
    private static final Logger log = Logger.get("TestQueries");

    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private final TestingPrestoServer prestoServer;
    private final ConnectorSession defaultSession;

    private final HttpClient httpClient;

    public TestingPrestoClient(TestingPrestoServer prestoServer, ConnectorSession defaultSession)
    {
        this.prestoServer = checkNotNull(prestoServer, "prestoServer is null");
        this.defaultSession = checkNotNull(defaultSession, "defaultSession is null");

        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setReadTimeout(new Duration(10, TimeUnit.DAYS)));
    }

    @Override
    public void close()
    {
        this.httpClient.close();
    }

    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    public MaterializedResult execute(ConnectorSession session, @Language("SQL") String sql)
    {
        try (StatementClient client = new StatementClient(httpClient, QUERY_RESULTS_CODEC, toClientSession(session), sql)) {
            AtomicBoolean loggedUri = new AtomicBoolean(false);
            ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
            List<Type> types = null;

            while (client.isValid()) {
                QueryResults results = client.current();
                if (!loggedUri.getAndSet(true)) {
                    log.info("Query %s: %s?pretty", results.getId(), results.getInfoUri());
                }

                if ((types == null) && (results.getColumns() != null)) {
                    types = getTypes(prestoServer.getMetadata(), results.getColumns());
                }
                if (results.getData() != null) {
                    rows.addAll(transform(results.getData(), dataToRow(session.getTimeZoneKey(), types)));
                }

                client.advance();
            }

            if (!client.isFailed()) {
                return new MaterializedResult(rows.build(), types);
            }

            QueryError error = client.finalResults().getError();
            assert error != null;
            if (error.getFailureInfo() != null) {
                throw error.getFailureInfo().toException();
            }
            throw new RuntimeException("Query failed: " + error.getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    public List<QualifiedTableName> listTables(ConnectorSession session, String catalog, String schema)
    {
        return prestoServer.getMetadata().listTables(session, new QualifiedTablePrefix(catalog, schema));
    }

    public boolean tableExists(ConnectorSession session, String table)
    {
        QualifiedTableName name = new QualifiedTableName(session.getCatalog(), session.getSchema(), table);
        Optional<TableHandle> handle = prestoServer.getMetadata().getTableHandle(session, name);
        return handle.isPresent();
    }

    public ConnectorSession getDefaultSession()
    {
        return defaultSession;
    }

    public TestingPrestoServer getServer()
    {
        return prestoServer;
    }

    public ClientSession toClientSession(ConnectorSession connectorSession)
    {
        return new ClientSession(
                prestoServer.getBaseUrl(),
                connectorSession.getUser(),
                connectorSession.getSource(),
                connectorSession.getCatalog(),
                connectorSession.getSchema(),
                connectorSession.getTimeZoneKey().getId(),
                connectorSession.getLocale(), true);
    }

    private static Function<List<Object>, MaterializedRow> dataToRow(final TimeZoneKey timeZoneKey, final List<Type> types)
    {
        return new Function<List<Object>, MaterializedRow>()
        {
            @Override
            public MaterializedRow apply(List<Object> data)
            {
                checkArgument(data.size() == types.size(), "columns size does not match types size");
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < data.size(); i++) {
                    Object value = data.get(i);
                    if (value == null) {
                        row.add(null);
                        continue;
                    }

                    Type type = types.get(i);
                    if (BOOLEAN.equals(type)) {
                        row.add(value);
                    }
                    else if (BIGINT.equals(type)) {
                        row.add(((Number) value).longValue());
                    }
                    else if (DOUBLE.equals(type)) {
                        row.add(((Number) value).doubleValue());
                    }
                    else if (VARCHAR.equals(type)) {
                        row.add(value);
                    }
                    else if (VARBINARY.equals(type)) {
                        row.add(value);
                    }
                    else if (DATE.equals(type)) {
                        row.add(new Date(parseDate((String) value)));
                    }
                    else if (TIME.equals(type)) {
                        row.add(new Time(parseTime(timeZoneKey, (String) value)));
                    }
                    else if (TIME_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Time(unpackMillisUtc(parseTimeWithTimeZone((String) value))));
                    }
                    else if (TIMESTAMP.equals(type)) {
                        row.add(new Timestamp(parseTimestamp(timeZoneKey, (String) value)));
                    }
                    else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Timestamp(unpackMillisUtc(parseTimestampWithTimeZone((String) value))));
                    }
                    else {
                        throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedRow(DEFAULT_PRECISION, row);
            }
        };
    }

    private static List<Type> getTypes(Metadata metadata, List<Column> columns)
    {
        return ImmutableList.copyOf(transform(columns, columnTypeGetter(metadata)));
    }

    private static Function<Column, Type> columnTypeGetter(final Metadata metadata)
    {
        return new Function<Column, Type>()
        {
            @Override
            public Type apply(Column column)
            {
                String typeName = column.getType();
                Type type = metadata.getType(typeName);
                if (type == null) {
                    throw new AssertionError("Unhandled type: " + typeName);
                }
                return type;
            }
        };
    }
}
