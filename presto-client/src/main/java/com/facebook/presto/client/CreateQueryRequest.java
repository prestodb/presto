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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class CreateQueryRequest
{
    private final CreateQuerySession session;
    private final String query;

    @JsonCreator
    public CreateQueryRequest(
            @JsonProperty("session") CreateQuerySession session,
            @JsonProperty("query") String query)
    {
        checkArgument(!isNullOrEmpty(query), "query is null or empty");
        this.query = query;
        this.session = requireNonNull(session, "session is null");
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public CreateQuerySession getSession()
    {
        return session;
    }
}
