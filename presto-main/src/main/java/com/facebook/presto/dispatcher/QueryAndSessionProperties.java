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

import java.util.Map;
import java.util.Optional;

public class QueryAndSessionProperties
{
    private final Optional<String> query;
    private final Map<String, String> systemSessionProperties;

    public QueryAndSessionProperties(Optional<String> query, Map<String, String> systemSessionProperties)
    {
        this.query = query;
        this.systemSessionProperties = systemSessionProperties;
    }

    public Optional<String> getQuery()
    {
        return query;
    }

    public Map<String, String> getSystemSessionProperties()
    {
        return systemSessionProperties;
    }
}
