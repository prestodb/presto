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
package com.facebook.presto.sql.analyzer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class Session
{
    public static final String DEFAULT_CATALOG = "default";
    public static final String DEFAULT_SCHEMA = "default";

    private final String user;
    private final String source;
    private final String remoteUserAddress;
    private final String userAgent;
    private final String catalog;
    private final String schema;
    private final long startTime;

    public Session(@Nullable String user, String source, String catalog, String schema, String remoteUserAddress, String userAgent)
    {
        this(user, source, catalog, schema, remoteUserAddress, userAgent, System.currentTimeMillis());
    }

    @JsonCreator
    public Session(
            @JsonProperty("user") @Nullable String user,
            @JsonProperty("source") String source,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("remoteUserAddress") String remoteUserAddress,
            @JsonProperty("userAgent") String userAgent,
            @JsonProperty("startTime") long startTime)
    {
        this.user = user;
        this.source = source;
        this.catalog = checkNotNull(catalog, "catalog is null");
        this.schema = checkNotNull(schema, "schema is null");
        this.remoteUserAddress = remoteUserAddress;
        this.userAgent = userAgent;
        this.startTime = startTime;
    }

    @JsonProperty
    @Nullable
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @JsonProperty
    public String getUserAgent()
    {
        return userAgent;
    }

    @JsonProperty
    public long getStartTime()
    {
        return startTime;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("user", user)
                .add("source", source)
                .add("remoteUserAddress", remoteUserAddress)
                .add("userAgent", userAgent)
                .add("catalog", catalog)
                .add("schema", schema)
                .add("startTime", startTime)
                .toString();
    }
}
