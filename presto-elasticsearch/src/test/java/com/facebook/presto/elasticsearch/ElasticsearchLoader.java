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

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.tests.AbstractTestingPrestoClient;
import com.facebook.presto.tests.ResultsSession;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
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

public class ElasticsearchLoader
        extends AbstractTestingPrestoClient<Void>
{
    private final String tableName;
    Rest5Client restClient;

    public ElasticsearchLoader(
            Rest5Client client,
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
        public void setWarnings(List<PrestoWarning> warnings)
        {
        }

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

            StringBuilder bulkPayload = new StringBuilder();
            ObjectMapper mapper = new ObjectMapper();

            for (List<Object> fields : data.getData()) {
                Map<String, Object> doc = new HashMap<>();
                for (int i = 0; i < fields.size(); i++) {
                    Type type = types.get().get(i);
                    Object value = convertValue(fields.get(i), type);
                    doc.put(columns.get(i).getName(), value);
                }

                bulkPayload.append("{\"index\":{\"_index\":\"").append(tableName).append("\"}}\n");
                try {
                    bulkPayload.append(mapper.writeValueAsString(doc)).append("\n");
                }
                catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            Request request = new Request("POST", "/_bulk");
            request.setJsonEntity(bulkPayload.toString());
            request.addParameter("refresh", "true");

            try {
                restClient.performRequest(request);
            }
            catch (IOException e) {
                throw new RuntimeException("Error loading data into Elasticsearch index: " + tableName, e);
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
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
