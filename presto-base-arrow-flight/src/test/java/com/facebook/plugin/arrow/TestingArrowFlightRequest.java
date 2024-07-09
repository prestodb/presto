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
package com.facebook.plugin.arrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class TestingArrowFlightRequest
        implements ArrowFlightRequest
{
    private final String schema;
    private final String table;
    private final Optional<String> query;
    private final ArrowFlightConfig config;
    private final int noOfPartitions;

    private final TestingArrowFlightConfig testconfig;

    public TestingArrowFlightRequest(ArrowFlightConfig config, TestingArrowFlightConfig testconfig, String schema, String table, Optional<String> query, int noOfPartitions)
    {
        this.config = config;
        this.schema = schema;
        this.table = table;
        this.query = query;
        this.testconfig = testconfig;
        this.noOfPartitions = noOfPartitions;
    }

    public TestingArrowFlightRequest(ArrowFlightConfig config, String schema, int noOfPartitions, TestingArrowFlightConfig testconfig)
    {
        this.schema = schema;
        this.table = null;
        this.query = Optional.empty();
        this.config = config;
        this.testconfig = testconfig;
        this.noOfPartitions = noOfPartitions;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public Optional<String> getQuery()
    {
        return query;
    }

    public TestingRequestData build()
    {
        TestingRequestData requestData = new TestingRequestData();
        requestData.setConnectionProperties(getConnectionProperties());
        requestData.setInteractionProperties(createInteractionProperties());
        return requestData;
    }

    @Override
    public byte[] getCommand()
    {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            String jsonString = objectMapper.writeValueAsString(build());
            return jsonString.getBytes(StandardCharsets.UTF_8);
        }
        catch (JsonProcessingException e) {
            throw new ArrowException(ArrowErrorCode.ARROW_FLIGHT_ERROR, "JSON request cannot be created.", e);
        }
    }

    private TestingConnectionProperties getConnectionProperties()
    {
        TestingConnectionProperties properties = new TestingConnectionProperties();
        properties.database = testconfig.getDataSourceDatabase();
        properties.host = testconfig.getDataSourceHost();
        properties.port = testconfig.getDataSourcePort();
        properties.username = testconfig.getDataSourceUsername();
        properties.password = testconfig.getDataSourcePassword();
        return properties;
    }

    private TestingInteractionProperties createInteractionProperties()
    {
        TestingInteractionProperties interactionProperties = new TestingInteractionProperties();
        if (getQuery().isPresent()) {
            interactionProperties.setSelectStatement(getQuery().get());
        }
        else {
            interactionProperties.setSchema(getSchema());
            interactionProperties.setTable(getTable());
        }
        return interactionProperties;
    }
}
