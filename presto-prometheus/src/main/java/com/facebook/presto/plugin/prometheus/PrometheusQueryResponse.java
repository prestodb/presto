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
package com.facebook.presto.plugin.prometheus;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_PARSE_ERROR;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static java.util.Collections.singletonList;

public class PrometheusQueryResponse
{
    enum ResultType {
        matrix,
        vector,
        scalar,
        string
    }
    private boolean status;

    private String error;
    private String errorType;
    private ResultType resultType;
    private String result;
    private List<PrometheusMetricResult> results;
    private final String parseResultStatus = "status";
    private final String parseResultSuccess = "success";
    private final String parseResultType = "resultType";
    private final String parseResult = "result";

    public PrometheusQueryResponse(InputStream response)
            throws IOException
    {
        parsePrometheusQueryResponse(response);
    }

    private void parsePrometheusQueryResponse(InputStream response)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        JsonParser parser = new JsonFactory().createParser(response);
        while (!parser.isClosed()) {
            JsonToken jsonToken = parser.nextToken();
            if (FIELD_NAME.equals(jsonToken)) {
                if (parser.getCurrentName().equals(parseResultStatus)) {
                    parser.nextToken();
                    if (parser.getValueAsString().equals(parseResultSuccess)) {
                        this.status = true;
                        while (!parser.isClosed()) {
                            parser.nextToken();
                            if (FIELD_NAME.equals(jsonToken)) {
                                if (parser.getCurrentName().equals(parseResultType)) {
                                    parser.nextToken();
                                    resultType = ResultType.valueOf(parser.getValueAsString());
                                }
                                if (parser.getCurrentName().equals(parseResult)) {
                                    parser.nextToken();
                                    ArrayNode node = mapper.readTree(parser);
                                    result = node.toString();
                                    break;
                                }
                            }
                        }
                    }
                    else {
                        //error path
                        String parsedStatus = parser.getValueAsString();
                        //parsing json is key-value based, so first nextToken is advanced to the key, nextToken advances to the value.
                        parser.nextToken(); // for "errorType" key
                        parser.nextToken(); // for "errorType" key's value
                        errorType = parser.getValueAsString();
                        parser.nextToken(); // advance to "error" key
                        parser.nextToken(); // advance to "error" key's value
                        error = parser.getValueAsString();
                        throw new PrestoException(PROMETHEUS_PARSE_ERROR, "Unable to parse Prometheus response: " + parsedStatus + " " + errorType + " " + error);
                    }
                }
            }
            if (result != null) {
                break;
            }
        }
        if (result != null && resultType != null) {
            switch (resultType) {
                case matrix:
                case vector:
                    results = mapper.readValue(result, new TypeReference<List<PrometheusMetricResult>>() {});
                    break;
                case scalar:
                case string:
                    PrometheusTimeSeriesValue stringOrScalarResult = mapper.readValue(result, new TypeReference<PrometheusTimeSeriesValue>() {});
                    Map<String, String> madeUpMetricHeader = new HashMap<>();
                    madeUpMetricHeader.put("__name__", resultType.toString());
                    PrometheusTimeSeriesValueArray timeSeriesValues = new PrometheusTimeSeriesValueArray(singletonList(stringOrScalarResult));
                    results = singletonList(new PrometheusMetricResult(madeUpMetricHeader, timeSeriesValues));
            }
        }
    }

    public String getError()
    {
        return error;
    }

    public String getErrorType()
    {
        return errorType;
    }

    public List<PrometheusMetricResult> getResults()
    {
        return results;
    }

    public boolean getStatus()
    {
        return this.status;
    }
}
