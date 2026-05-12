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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MaterializedViewStatistics
{
    private final List<MaterializedViewQueryInfo> queryInfos;
    private final List<MaterializedViewRewriteInfo> rewriteInfos;

    @JsonCreator
    public MaterializedViewStatistics(
            @JsonProperty("queryInfos") List<MaterializedViewQueryInfo> queryInfos,
            @JsonProperty("rewriteInfos") List<MaterializedViewRewriteInfo> rewriteInfos)
    {
        this.queryInfos = requireNonNull(queryInfos, "queryInfos is null");
        this.rewriteInfos = requireNonNull(rewriteInfos, "rewriteInfos is null");
    }

    @JsonProperty
    public List<MaterializedViewQueryInfo> getQueryInfos()
    {
        return queryInfos;
    }

    @JsonProperty
    public List<MaterializedViewRewriteInfo> getRewriteInfos()
    {
        return rewriteInfos;
    }

    public boolean isEmpty()
    {
        return queryInfos.isEmpty() && rewriteInfos.isEmpty();
    }
}
