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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowColumnHandle;
import com.facebook.plugin.arrow.ArrowTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
public class TestingQueryArrowTableHandle
        extends ArrowTableHandle
{
    private final String query;
    private final List<ArrowColumnHandle> columns;

    @JsonCreator
    public TestingQueryArrowTableHandle(String query, List<ArrowColumnHandle> columns)
    {
        super("schema-" + UUID.randomUUID(), "table-" + UUID.randomUUID());
        this.columns = Collections.unmodifiableList(requireNonNull(columns));
        this.query = requireNonNull(query);
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public List<ArrowColumnHandle> getColumns()
    {
        return columns;
    }
}
