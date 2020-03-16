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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
@EventType("DeterminismAnalysisRun")
public class DeterminismAnalysisRun
{
    private final String tableName;
    private final String queryId;
    private final String checksumQueryId;

    private DeterminismAnalysisRun(
            Optional<String> tableName,
            Optional<String> queryId,
            Optional<String> checksumQueryId)
    {
        this.tableName = tableName.orElse(null);
        this.queryId = queryId.orElse(null);
        this.checksumQueryId = checksumQueryId.orElse(null);
    }

    @EventField
    public String getTableName()
    {
        return tableName;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public String getChecksumQueryId()
    {
        return checksumQueryId;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String tableName;
        private String queryId;
        private String checksumQueryId;

        private Builder()
        {
        }

        public Builder setTableName(String tableName)
        {
            checkState(this.tableName == null, "tableName is already set");
            this.tableName = requireNonNull(tableName, "tableName is null");
            return this;
        }

        public Builder setQueryId(String queryId)
        {
            checkState(this.queryId == null, "queryId is already set");
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        public Builder setChecksumQueryId(String checksumQueryId)
        {
            checkState(this.checksumQueryId == null, "checksumQueryId is already set");
            this.checksumQueryId = requireNonNull(checksumQueryId, "checksumQueryId is null");
            return this;
        }

        public DeterminismAnalysisRun build()
        {
            return new DeterminismAnalysisRun(Optional.ofNullable(tableName), Optional.ofNullable(queryId), Optional.ofNullable(checksumQueryId));
        }
    }
}
