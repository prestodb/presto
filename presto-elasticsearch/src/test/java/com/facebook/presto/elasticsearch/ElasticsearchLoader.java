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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.tests.AbstractTestingPrestoClient;
import com.facebook.presto.tests.ResultsSession;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.client.RequestOptions.DEFAULT;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchLoader
        extends AbstractTestingPrestoClient<Void>
{
    private final String tableName;
    RestHighLevelClient restClient;

    public ElasticsearchLoader(
            RestHighLevelClient client,
            String tableName,
            TestingPrestoServer prestoServer,
            Session defaultSession)
    {
        super(prestoServer, defaultSession);

        this.tableName = requireNonNull(tableName, "tableName is null");
        this.restClient = requireNonNull(client, "client is null");
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new ElasticsearchLoadingSession();
    }

    private class ElasticsearchLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private ElasticsearchLoadingSession() {}

        @Override
        public void setWarnings(List<PrestoWarning> warnings) {}

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() == null) {
                return;
            }
            checkState(types.get() != null, "Type information is missing");
            List<Column> columns = statusInfo.getColumns();
            BulkRequest request = new BulkRequest();
            for (List<Object> fields : data.getData()) {
                try {
                    XContentBuilder dataBuilder = jsonBuilder().startObject();
                    for (int i = 0; i < fields.size(); i++) {
                        Type type = types.get().get(i);
                        Object value = convertValue(fields.get(i), type);
                        dataBuilder.field(columns.get(i).getName(), value);
                    }
                    dataBuilder.endObject();
                    request.add(new IndexRequest(tableName, "_doc").source(dataBuilder));
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error loading data into Elasticsearch index: " + tableName, e);
                }
            }
            request.setRefreshPolicy(IMMEDIATE);
            try {
                restClient.bulk(request, DEFAULT);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties, String startTransactionId, boolean clearTransactionId)
        {
            return null;
        }

        private Object convertValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }

            if (type == BOOLEAN || type == DATE || isVarcharType(type)) {
                return value;
            }
            if (type == BIGINT) {
                return ((Number) value).longValue();
            }
            if (type == INTEGER) {
                return ((Number) value).intValue();
            }
            if (type.equals(DOUBLE)) {
                return ((Number) value).doubleValue();
            }
            throw new IllegalArgumentException("Unhandled type: " + type);
        }
    }
}
