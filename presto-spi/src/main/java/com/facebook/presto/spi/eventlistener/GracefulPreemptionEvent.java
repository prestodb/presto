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

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

@Immutable
public class GracefulPreemptionEvent
{
    private final String queryId;
    private final String taskId;
    private final long eventTime;
    private final String state;
    private final Optional<Long> outputBufferSize;
    private final Optional<String> outputBufferID;

    public GracefulPreemptionEvent(String queryId, String taskId, long eventTime, String state, Optional<Long> outputBufferSize, Optional<String> outputBufferID)
    {
        this.queryId = queryId;
        this.taskId = taskId;
        this.eventTime = eventTime;
        this.state = state;
        this.outputBufferID = outputBufferID;
        this.outputBufferSize = outputBufferSize;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public long getEventTime()
    {
        return eventTime;
    }

    public String getState()
    {
        return state;
    }

    public Optional<Long> getOutputBufferSize()
    {
        return outputBufferSize;
    }

    public Optional<String> getOutputBufferID()
    {
        return outputBufferID;
    }
}
