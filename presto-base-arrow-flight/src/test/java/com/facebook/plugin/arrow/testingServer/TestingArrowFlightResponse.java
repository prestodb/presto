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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TestingArrowFlightResponse
{
    private final List<String> schemaNames;
    private final List<String> tableNames;

    @JsonCreator
    public TestingArrowFlightResponse(@JsonProperty("schemaNames") List<String> schemaNames, @JsonProperty("tableNames") List<String> tableNames)
    {
        this.schemaNames = ImmutableList.copyOf(requireNonNull(schemaNames, "schemaNames is null"));
        this.tableNames = ImmutableList.copyOf(requireNonNull(tableNames, "tableNames is null"));
    }

    @JsonProperty
    public List<String> getSchemaNames()
    {
        return schemaNames;
    }

    @JsonProperty
    public List<String> getTableNames()
    {
        return tableNames;
    }
}
