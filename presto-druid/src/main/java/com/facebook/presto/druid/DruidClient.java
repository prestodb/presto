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
package com.facebook.presto.druid;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.druid.metadata.DruidColumnInfo;
import com.facebook.presto.druid.metadata.DruidSegmentIdWrapper;
import com.facebook.presto.druid.metadata.DruidSegmentInfo;
import com.facebook.presto.druid.metadata.DruidTableInfo;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.druid.DruidErrorCode.DRUID_BROKER_RESULT_ERROR;
import static com.facebook.presto.druid.DruidResultFormat.ARRAY_LINES;
import static com.facebook.presto.druid.DruidResultFormat.OBJECT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
    private static final String METADATA_PATH = "/druid/coordinator/v1/metadata";
    private static final String SQL_ENDPOINT = "/druid/v2/sql";
    private static final String APPLICATION_JSON = "application/json";
    private static final String LIST_TABLE_QUERY = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'";
    private static final String GET_COLUMN_TEMPLATE = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '%s'";
    private static final String GET_SEGMENTS_ID_TEMPLATE = "SELECT segment_id FROM sys.segments WHERE datasource = '%s' AND is_published = 1";

    private static final JsonCodec<List<DruidSegmentIdWrapper>> LIST_SEGMENT_ID_CODEC = listJsonCodec(DruidSegmentIdWrapper.class);
    private static final JsonCodec<List<DruidColumnInfo>> LIST_COLUMN_INFO_CODEC = listJsonCodec(DruidColumnInfo.class);
    private static final JsonCodec<List<DruidTableInfo>> LIST_TABLE_NAME_CODEC = listJsonCodec(DruidTableInfo.class);
    private static final JsonCodec<DruidSegmentInfo> SEGMENT_INFO_CODEC = jsonCodec(DruidSegmentInfo.class);

    private final HttpClient httpClient;
    private final URI druidCoordinator;
    private final URI druidBroker;
    private final String druidSchema;

    @Inject
    public DruidClient(DruidConfig config, @ForDruidClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.druidCoordinator = URI.create(config.getDruidCoordinatorUrl());
        this.druidBroker = URI.create(config.getDruidBrokerUrl());
        this.druidSchema = config.getDruidSchema();
    }

    public URI getDruidBroker()
    {
        return druidBroker;
    }

    public String getSchema()
    {
        return druidSchema;
    }

    public List<String> getSchemas()
    {
        return ImmutableList.of(druidSchema);
    }

    public List<String> getTables()
    {
        return httpClient.execute(prepareMetadataQuery(LIST_TABLE_QUERY), createJsonResponseHandler(LIST_TABLE_NAME_CODEC)).stream()
                .map(DruidTableInfo::getTableName)
                .collect(toImmutableList());
    }

    public List<DruidColumnInfo> getColumnDataType(String tableName)
    {
        return httpClient.execute(prepareMetadataQuery(format(GET_COLUMN_TEMPLATE, tableName)), createJsonResponseHandler(LIST_COLUMN_INFO_CODEC));
    }

    public List<String> getDataSegmentId(String tableName)
    {
        return httpClient.execute(prepareMetadataQuery(format(GET_SEGMENTS_ID_TEMPLATE, tableName)), createJsonResponseHandler(LIST_SEGMENT_ID_CODEC)).stream()
                .map(wrapper -> wrapper.getSegmentId())
                .collect(toImmutableList());
    }

    public DruidSegmentInfo getSingleSegmentInfo(String dataSource, String segmentId)
    {
        URI uri = uriBuilderFrom(druidCoordinator)
                .replacePath(METADATA_PATH)
                .appendPath(format("datasources/%s/segments/%s", dataSource, segmentId))
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(SEGMENT_INFO_CODEC));
    }

    public InputStream getData(String dql)
    {
        return httpClient.execute(prepareDataQuery(dql), new StreamingJsonResponseHandler());
    }

    private static Request.Builder setContentTypeHeaders(Request.Builder requestBuilder)
    {
        return requestBuilder
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString());
    }

    private static byte[] createRequestBody(String query, DruidResultFormat resultFormat, boolean queryHeader)
    {
        return new DruidRequestBody(query, resultFormat.getResultFormat(), queryHeader).toJson().getBytes();
    }

    private Request prepareMetadataQuery(String query)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(druidBroker).replacePath(SQL_ENDPOINT);

        return setContentTypeHeaders(preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(createRequestBody(query, OBJECT, false)))
                .build();
    }

    private Request prepareDataQuery(String query)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(druidBroker).replacePath(SQL_ENDPOINT);

        return setContentTypeHeaders(preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(createRequestBody(query, ARRAY_LINES, false)))
                .build();
    }

    private static class StreamingJsonResponseHandler
            implements ResponseHandler<InputStream, RuntimeException>
    {
        @Override
        public InputStream handleException(Request request, Exception exception)
        {
            throw new PrestoException(DRUID_BROKER_RESULT_ERROR, "Request to worker failed", exception);
        }

        @Override
        public InputStream handle(Request request, com.facebook.airlift.http.client.Response response)
        {
            try {
                if (APPLICATION_JSON.equals(response.getHeader(CONTENT_TYPE))) {
                    return response.getInputStream();
                }
                throw new PrestoException(DRUID_BROKER_RESULT_ERROR, "Response received was not of type " + APPLICATION_JSON);
            }
            catch (IOException e) {
                throw new PrestoException(DRUID_BROKER_RESULT_ERROR, "Unable to read response from worker", e);
            }
        }
    }
    public static class DruidRequestBody
    {
        private String query;
        private String resultFormat;
        private boolean queryHeader;

        @JsonCreator
        public DruidRequestBody(
                @JsonProperty("query") String query,
                @JsonProperty("resultFormat") String resultFormat,
                @JsonProperty("queryHeader") boolean queryHeader)
        {
            this.query = requireNonNull(query);
            this.resultFormat = requireNonNull(resultFormat);
            this.queryHeader = queryHeader;
        }

        @JsonProperty("query")
        public String getQuery()
        {
            return query;
        }

        @JsonProperty("resultFormat")
        public String getResultFormat()
        {
            return resultFormat;
        }

        @JsonProperty("queryHeader")
        public boolean isQueryHeader()
        {
            return queryHeader;
        }

        public String toJson()
        {
            return JsonCodec.jsonCodec(DruidRequestBody.class).toJson(this);
        }
    }
}
