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

import java.util.Optional;

public abstract class ArrowAbstractFlightRequest
        implements ArrowFlightRequest
{
    private final String schema;
    private final String table;
    private final Optional<String> query;

    public ArrowAbstractFlightRequest(String schema)
    {
        this.schema = schema;
        this.query = Optional.empty();
        this.table = null;
    }

    public ArrowAbstractFlightRequest(String schema, String table)
    {
        this.schema = schema;
        this.table = table;
        query = Optional.empty();
    }

    public ArrowAbstractFlightRequest(String schema, String table, Optional<String> query)
    {
        this.schema = schema;
        this.table = table;
        this.query = query;
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
}
