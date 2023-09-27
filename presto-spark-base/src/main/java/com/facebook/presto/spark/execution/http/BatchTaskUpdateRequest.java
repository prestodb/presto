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
package com.facebook.presto.spark.execution.http;

import com.facebook.presto.server.TaskUpdateRequest;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

// Marked as final to prevent finalizer attack. See Guideline 7-3 in java secure coding guidelines
public final class BatchTaskUpdateRequest
{
    private final TaskUpdateRequest taskUpdateRequest;
    private final Optional<String> shuffleWriteInfo;
    private final Optional<String> broadcastBasePath;

    @JsonCreator
    public BatchTaskUpdateRequest(
            @JsonProperty("taskUpdateRequest") TaskUpdateRequest taskUpdateRequest,
            @JsonProperty("shuffleWriteInfo") Optional<String> shuffleWriteInfo,
            @JsonProperty("broadcastBasePath") Optional<String> broadcastBasePath)
    {
        this.taskUpdateRequest = requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");
        this.shuffleWriteInfo = requireNonNull(shuffleWriteInfo, "shuffleWriteInfo is null");
        this.broadcastBasePath = requireNonNull(broadcastBasePath, "broadcastBasePath is null");

        // shuffleWriteInfo and broadcastBasePath, both can't have value at the same time.
        if (this.shuffleWriteInfo.isPresent() && this.broadcastBasePath.isPresent()) {
            throw new IllegalArgumentException("shuffleWriteInfo and broadcastBasePath can not be specified in same request");
        }
    }

    @JsonProperty
    public TaskUpdateRequest getTaskUpdateRequest()
    {
        return taskUpdateRequest;
    }

    @JsonProperty
    public Optional<String> getShuffleWriteInfo()
    {
        return shuffleWriteInfo;
    }

    @JsonProperty
    public Optional<String> getBroadcastBasePath()
    {
        return broadcastBasePath;
    }

    @Override
    public String toString()
    {
        ToStringHelper stringHelper = toStringHelper(this)
                .add("taskUpdateRequest", taskUpdateRequest);
        shuffleWriteInfo.ifPresent(shuffleInfo -> stringHelper.add("shuffleWriteInfo", shuffleInfo));
        broadcastBasePath.ifPresent(basePath -> stringHelper.add("broadcastBasePath", basePath));
        return stringHelper.toString();
    }
}
