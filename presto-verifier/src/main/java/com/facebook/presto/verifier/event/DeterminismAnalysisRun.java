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
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

@Immutable
@EventType("DeterminismAnalysisRun")
public class DeterminismAnalysisRun
{
    private final String tableName;
    private final String queryId;
    private final List<String> setupQueryIds;
    private final List<String> teardownQueryIds;
    private final String checksumQueryId;

    private DeterminismAnalysisRun(
            Optional<String> tableName,
            Optional<String> queryId,
            List<String> setupQueryIds,
            List<String> teardownQueryIds,
            Optional<String> checksumQueryId)
    {
        this.tableName = tableName.orElse(null);
        this.queryId = queryId.orElse(null);
        this.setupQueryIds = ImmutableList.copyOf(setupQueryIds);
        this.teardownQueryIds = ImmutableList.copyOf(teardownQueryIds);
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
    public List<String> getSetupQueryIds()
    {
        return setupQueryIds;
    }

    @EventField
    public List<String> getTeardownQueryIds()
    {
        return teardownQueryIds;
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
        private Optional<String> tableName = Optional.empty();
        private Optional<String> queryId = Optional.empty();
        private ImmutableList.Builder<String> setupQueryIds = ImmutableList.builder();
        private ImmutableList.Builder<String> teardownQueryIds = ImmutableList.builder();
        private Optional<String> checksumQueryId = Optional.empty();

        private Builder() {}

        public Builder setTableName(String tableName)
        {
            checkState(!this.tableName.isPresent(), "tableName is already set");
            this.tableName = Optional.of(tableName);
            return this;
        }

        public Builder setQueryId(String queryId)
        {
            checkState(!this.queryId.isPresent(), "queryId is already set");
            this.queryId = Optional.of(queryId);
            return this;
        }

        public Builder addSetupQueryId(String queryId)
        {
            this.setupQueryIds.add(queryId);
            return this;
        }

        public Builder addTeardownQueryId(String queryId)
        {
            this.teardownQueryIds.add(queryId);
            return this;
        }

        public Builder setChecksumQueryId(String checksumQueryId)
        {
            checkState(!this.checksumQueryId.isPresent(), "checksumQueryId is already set");
            this.checksumQueryId = Optional.of(checksumQueryId);
            return this;
        }

        public DeterminismAnalysisRun build()
        {
            return new DeterminismAnalysisRun(tableName, queryId, setupQueryIds.build(), teardownQueryIds.build(), checksumQueryId);
        }
    }
}
