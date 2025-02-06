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

package com.facebook.presto.dispatcher;

import com.facebook.presto.common.analyzer.PreparedQuery;

import java.util.Map;
import java.util.Optional;

public class QueryAndSessionProperties
{
    private final String query;
    private final Map<String, String> systemSessionProperties;
    private final Optional<PreparedQuery> preparedQuery;

    public QueryAndSessionProperties(String query, Map<String, String> systemSessionProperties, Optional<PreparedQuery> preparedQuery)
    {
        this.query = query;
        this.systemSessionProperties = systemSessionProperties;
        this.preparedQuery = preparedQuery;
    }

    public String getQuery()
    {
        return query;
    }

    public Map<String, String> getSystemSessionProperties()
    {
        return systemSessionProperties;
    }

    public Optional<PreparedQuery> getPreparedQuery()
    {
        return preparedQuery;
    }
}
