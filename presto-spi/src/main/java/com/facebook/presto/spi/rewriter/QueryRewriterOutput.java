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

package com.facebook.presto.spi.rewriter;

import com.facebook.presto.common.RuntimeStats;

import java.util.Map;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class QueryRewriterOutput
{
    private final String queryId;
    private final String originalQuery;
    private final String rewrittenQuery;
    private final Map<String, String> sessionProperties;
    private final RuntimeStats runtimeStats;

    private QueryRewriterOutput(String queryId, String originalQuery, String rewrittenQuery, Map<String, String> sessionProperties, RuntimeStats runtimeStats)
    {
        this.queryId = queryId;
        this.originalQuery = originalQuery;
        this.rewrittenQuery = rewrittenQuery;
        this.sessionProperties = sessionProperties;
        this.runtimeStats = runtimeStats;
    }

    public String getOriginalQuery()
    {
        return originalQuery;
    }

    public String getRewrittenQuery()
    {
        return rewrittenQuery;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public String toString()
    {
        return "QueryRewriterOutput{" +
                "queryId='" + queryId + '\'' +
                ", originalQuery='" + originalQuery + '\'' +
                ", rewrittenQuery='" + rewrittenQuery + '\'' +
                ", sessionProperties=" + sessionProperties +
                '}';
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public static class Builder
    {
        private String queryId;
        private String originalQuery;
        private String rewrittenQuery;
        private Map<String, String> sessionProperties = emptyMap();
        private RuntimeStats runtimeStats;

        public Builder setQueryId(String queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        public Builder setOriginalQuery(String originalQuery)
        {
            this.originalQuery = requireNonNull(originalQuery, "originalQuery is null");
            return this;
        }

        public Builder setRewrittenQuery(String rewrittenQuery)
        {
            this.rewrittenQuery = requireNonNull(rewrittenQuery, "rewrittenQuery is null");
            return this;
        }

        public Builder setSessionProperties(Map<String, String> sessionProperties)
        {
            this.sessionProperties = sessionProperties;
            return this;
        }

        public Builder setRuntimeStats(RuntimeStats runtimeStats)
        {
            this.runtimeStats = runtimeStats;
            return this;
        }

        public QueryRewriterOutput build()
        {
            requireNonNull(rewrittenQuery, "rewrittenQuery is null");
            requireNonNull(originalQuery, "originalQuery is null");
            requireNonNull(queryId, "queryId is null");
            checkArgument(!rewrittenQuery.isEmpty() && !originalQuery.isEmpty() && !queryId.isEmpty());
            return new QueryRewriterOutput(queryId, originalQuery, rewrittenQuery, sessionProperties, runtimeStats);
        }
    }
}
