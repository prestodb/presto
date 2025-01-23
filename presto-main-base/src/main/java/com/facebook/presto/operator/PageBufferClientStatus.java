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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class PageBufferClientStatus
{
    private final URI uri;
    private final String state;
    private final DateTime lastUpdate;
    private final long rowsReceived;
    private final int pagesReceived;
    // use optional to keep the output size down, since this renders for every destination
    private final OptionalLong rowsRejected;
    private final OptionalInt pagesRejected;
    private final int requestsScheduled;
    private final int requestsCompleted;
    private final int requestsFailed;
    private final String httpRequestState;

    @JsonCreator
    @ThriftConstructor
    public PageBufferClientStatus(@JsonProperty("uri") URI uri,
            @JsonProperty("state") String state,
            @JsonProperty("lastUpdate") DateTime lastUpdate,
            @JsonProperty("rowsReceived") long rowsReceived,
            @JsonProperty("pagesReceived") int pagesReceived,
            @JsonProperty("rowsRejected") OptionalLong rowsRejected,
            @JsonProperty("pagesRejected") OptionalInt pagesRejected,
            @JsonProperty("requestsScheduled") int requestsScheduled,
            @JsonProperty("requestsCompleted") int requestsCompleted,
            @JsonProperty("requestsFailed") int requestsFailed,
            @JsonProperty("httpRequestState") String httpRequestState)
    {
        this.uri = uri;
        this.state = state;
        this.lastUpdate = lastUpdate;
        this.rowsReceived = rowsReceived;
        this.pagesReceived = pagesReceived;
        this.rowsRejected = requireNonNull(rowsRejected, "rowsRejected is null");
        this.pagesRejected = requireNonNull(pagesRejected, "pagesRejected is null");
        this.requestsScheduled = requestsScheduled;
        this.requestsCompleted = requestsCompleted;
        this.requestsFailed = requestsFailed;
        this.httpRequestState = httpRequestState;
    }

    @JsonProperty
    @ThriftField(1)
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    @ThriftField(2)
    public String getState()
    {
        return state;
    }

    @JsonProperty
    @ThriftField(3)
    public DateTime getLastUpdate()
    {
        return lastUpdate;
    }

    @JsonProperty
    @ThriftField(4)
    public long getRowsReceived()
    {
        return rowsReceived;
    }

    @JsonProperty
    @ThriftField(5)
    public int getPagesReceived()
    {
        return pagesReceived;
    }

    @JsonProperty
    @ThriftField(6)
    public OptionalLong getRowsRejected()
    {
        return rowsRejected;
    }

    @JsonProperty
    @ThriftField(7)
    public OptionalInt getPagesRejected()
    {
        return pagesRejected;
    }

    @JsonProperty
    @ThriftField(8)
    public int getRequestsScheduled()
    {
        return requestsScheduled;
    }

    @JsonProperty
    @ThriftField(9)
    public int getRequestsCompleted()
    {
        return requestsCompleted;
    }

    @JsonProperty
    @ThriftField(10)
    public int getRequestsFailed()
    {
        return requestsFailed;
    }

    @JsonProperty
    @ThriftField(11)
    public String getHttpRequestState()
    {
        return httpRequestState;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("uri", uri)
                .add("state", state)
                .add("lastUpdate", lastUpdate)
                .add("rowsReceived", rowsReceived)
                .add("pagesReceived", pagesReceived)
                .add("httpRequestState", httpRequestState)
                .toString();
    }
}
