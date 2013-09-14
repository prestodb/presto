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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.joda.time.DateTime;

import java.net.URI;

public class PageBufferClientStatus
{
    private final URI uri;
    private final String state;
    private final DateTime lastUpdate;
    private final int pagesReceived;
    private final int requestsScheduled;
    private final int requestsCompleted;
    private final String httpRequestState;

    @JsonCreator
    public PageBufferClientStatus(@JsonProperty("uri") URI uri,
            @JsonProperty("state") String state,
            @JsonProperty("lastUpdate") DateTime lastUpdate,
            @JsonProperty("pagesReceived") int pagesReceived,
            @JsonProperty("requestsScheduled") int requestsScheduled,
            @JsonProperty("requestsCompleted") int requestsCompleted,
            @JsonProperty("httpRequestState") String httpRequestState)
    {
        this.uri = uri;
        this.state = state;
        this.lastUpdate = lastUpdate;
        this.pagesReceived = pagesReceived;
        this.requestsScheduled = requestsScheduled;
        this.requestsCompleted = requestsCompleted;
        this.httpRequestState = httpRequestState;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public DateTime getLastUpdate()
    {
        return lastUpdate;
    }

    @JsonProperty
    public int getPagesReceived()
    {
        return pagesReceived;
    }

    @JsonProperty
    public int getRequestsScheduled()
    {
        return requestsScheduled;
    }

    @JsonProperty
    public int getRequestsCompleted()
    {
        return requestsCompleted;
    }

    @JsonProperty
    public String getHttpRequestState()
    {
        return httpRequestState;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("uri", uri)
                .add("state", state)
                .add("lastUpdate", lastUpdate)
                .add("pagesReceived", pagesReceived)
                .add("httpRequestState", httpRequestState)
                .toString();
    }

    public static Function<PageBufferClientStatus, URI> uriGetter()
    {
        return new Function<PageBufferClientStatus, URI>()
        {
            @Override
            public URI apply(PageBufferClientStatus input)
            {
                return input.getUri();
            }
        };
    }
}
