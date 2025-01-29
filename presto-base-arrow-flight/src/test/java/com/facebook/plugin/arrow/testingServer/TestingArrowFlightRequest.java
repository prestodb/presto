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
package com.facebook.plugin.arrow.testingServer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class TestingArrowFlightRequest
{
    private final Optional<String> schema;
    private final Optional<String> table;
    private final Optional<String> query;

    @JsonCreator
    public TestingArrowFlightRequest(
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("table") Optional<String> table,
            @JsonProperty("query") Optional<String> query)
    {
        this.schema = schema;
        this.table = table;
        this.query = query;
    }

    public static TestingArrowFlightRequest createListSchemaRequest()
    {
        return new TestingArrowFlightRequest(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static TestingArrowFlightRequest createListTablesRequest(String schema)
    {
        return new TestingArrowFlightRequest(Optional.of(schema), Optional.empty(), Optional.empty());
    }

    public static TestingArrowFlightRequest createDescribeTableRequest(String schema, String table)
    {
        return new TestingArrowFlightRequest(Optional.of(schema), Optional.of(table), Optional.empty());
    }

    public static TestingArrowFlightRequest createQueryRequest(String schema, String table, String query)
    {
        return new TestingArrowFlightRequest(Optional.of(schema), Optional.of(table), Optional.of(query));
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Optional<String> getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }
}
