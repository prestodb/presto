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
package com.facebook.presto.spi.statistics;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.lang.String.format;

@ThriftStruct
public class HistoricalPlanStatisticsEntryInfo
{
    private final WorkerType workerType;
    private final QueryId queryId;
    private final String serverVersion;

    @JsonCreator
    @ThriftConstructor
    public HistoricalPlanStatisticsEntryInfo(@JsonProperty("workerType") WorkerType workerType, @JsonProperty("queryId") QueryId queryId, @JsonProperty("serverVersion") String serverVersion)
    {
        this.workerType = workerType;
        this.queryId = queryId;
        this.serverVersion = serverVersion;
    }

    @JsonProperty
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public WorkerType getWorkerType()
    {
        return workerType;
    }

    @JsonProperty
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public String getServerVersion()
    {
        return serverVersion;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HistoricalPlanStatisticsEntryInfo that = (HistoricalPlanStatisticsEntryInfo) o;
        return Objects.equals(workerType, that.workerType) && Objects.equals(queryId, that.queryId) && Objects.equals(serverVersion, that.serverVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(workerType, queryId, serverVersion);
    }

    @Override
    public String toString()
    {
        return format("HistoricalPlanStatisticsEntryInfo{workerType=%s, queryId=%s, serverVersion=%s}", workerType, queryId, serverVersion);
    }

    @ThriftEnum
    public enum WorkerType
    {
        JAVA(1), CPP(2);

        private final int value;

        WorkerType(int value)
        {
            this.value = value;
        }

        @ThriftEnumValue
        public int getValue()
        {
            return value;
        }
    }
}
