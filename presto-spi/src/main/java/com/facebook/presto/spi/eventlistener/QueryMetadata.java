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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryMetadata
{
    private final String queryId;
    private final Optional<String> transactionId;

    private final String query;
    private final String queryState;

    private final URI uri;

    private final Optional<String> plan;

    private final Optional<String> jsonPlan;

    private final Optional<String> payload;

    private final List<String> runtimeOptimizedStages;

    public QueryMetadata(
            String queryId,
            Optional<String> transactionId,
            String query,
            String queryState,
            URI uri,
            Optional<String> plan,
            Optional<String> jsonPlan,
            Optional<String> payload,
            List<String> runtimeOptimizedStages)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.query = requireNonNull(query, "query is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.jsonPlan = requireNonNull(jsonPlan, "jsonPlan is null");
        this.payload = requireNonNull(payload, "payload is null");
        this.runtimeOptimizedStages = requireNonNull(runtimeOptimizedStages, "runtimeOptimizedStages is null");
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public Optional<String> getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getQueryState()
    {
        return queryState;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    public Optional<String> getPlan()
    {
        return plan;
    }

    @JsonProperty
    public Optional<String> getJsonPlan()
    {
        return jsonPlan;
    }

    @JsonProperty
    public Optional<String> getPayload()
    {
        return payload;
    }

    @JsonProperty
    public List<String> getRuntimeOptimizedStages()
    {
        return runtimeOptimizedStages;
    }
}
