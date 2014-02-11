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
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class QueryStats
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final StatementStats stats;
    private final QueryError error;

    @JsonCreator
    public QueryStats(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error)
    {
        this.id = checkNotNull(id, "id is null");
        this.infoUri = checkNotNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.stats = checkNotNull(stats, "stats is null");
        this.error = error;
    }

    @NotNull
    @JsonProperty
    public String getId()
    {
        return id;
    }

    @NotNull
    @JsonProperty
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @NotNull
    @JsonProperty
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    public QueryError getError()
    {
        return error;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("stats", stats)
                .add("error", error)
                .toString();
    }
}
