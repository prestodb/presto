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
package com.facebook.presto.server.remotetask;

import com.facebook.presto.common.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class DynamicFilterPushRequest
{
    private final boolean complete;
    private final String scanPlanNodeId;
    private final TupleDomain<String> tupleDomain;

    @JsonCreator
    public DynamicFilterPushRequest(
            @JsonProperty("complete") boolean complete,
            @JsonProperty("scanPlanNodeId") String scanPlanNodeId,
            @JsonProperty("tupleDomain") TupleDomain<String> tupleDomain)
    {
        this.complete = complete;
        this.scanPlanNodeId = requireNonNull(scanPlanNodeId, "scanPlanNodeId is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public boolean isComplete()
    {
        return complete;
    }

    @JsonProperty
    public String getScanPlanNodeId()
    {
        return scanPlanNodeId;
    }

    @JsonProperty
    public TupleDomain<String> getTupleDomain()
    {
        return tupleDomain;
    }
}
