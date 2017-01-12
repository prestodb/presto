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
package com.facebook.presto.execution;

import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.FailureInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class TaskFailureInfo
{
    private final FailureInfo failureInfo;
    private final Optional<String> failureTask;
    private final Optional<URI>  failureTaskUri;
    private final Optional<String> failureHost;
    private final Optional<Integer> failurePort;

    @JsonCreator
    public TaskFailureInfo(
            @JsonProperty("type") String type,
            @JsonProperty("message") String message,
            @JsonProperty("cause") FailureInfo cause,
            @JsonProperty("suppressed") List<FailureInfo> suppressed,
            @JsonProperty("stack") List<String> stack,
            @JsonProperty("errorLocation") @Nullable ErrorLocation errorLocation,
            @JsonProperty("task") Optional<String> task,
            @JsonProperty("uri") Optional<URI> uri,
            @JsonProperty("host") Optional<String> host,
            @JsonProperty("port") Optional<Integer> port)
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(task, "task is null");
        requireNonNull(host, "host is null");
        requireNonNull(port, "port is null");

        this.failureInfo = new FailureInfo(type, message, cause, suppressed, stack, errorLocation);
        this.failureTask = task;
        this.failureTaskUri = uri;
        this.failureHost = host;
        this.failurePort = port;
    }

    public TaskFailureInfo(FailureInfo failureInfo)
    {
        requireNonNull(failureInfo, "failureInfo is null");

        this.failureInfo = failureInfo;
        this.failureTask = Optional.empty();
        this.failureTaskUri = Optional.empty();
        this.failureHost = Optional.empty();
        this.failurePort = Optional.empty();
    }

    public TaskFailureInfo(FailureInfo failureInfo, Optional<String> task, Optional<URI> uri, Optional<String> host, Optional<Integer> port)
    {
        requireNonNull(failureInfo, "failureInfo is null");
        requireNonNull(task, "task is null");
        requireNonNull(uri, "uri is null");
        requireNonNull(host, "host is null");
        requireNonNull(port, "port is null");

        this.failureInfo = failureInfo;
        this.failureTask = task;
        this.failureTaskUri = uri;
        this.failureHost = host;
        this.failurePort = port;
    }

    @NotNull
    @JsonProperty
    public String getType()
    {
        return failureInfo.getType();
    }

    @Nullable
    @JsonProperty
    public String getMessage()
    {
        return failureInfo.getMessage();
    }

    @Nullable
    @JsonProperty
    public FailureInfo getCause()
    {
        return failureInfo.getCause();
    }

    @NotNull
    @JsonProperty
    public List<FailureInfo> getSuppressed()
    {
        return failureInfo.getSuppressed();
    }

    @NotNull
    @JsonProperty
    public List<String> getStack()
    {
        return failureInfo.getStack();
    }

    @Nullable
    @JsonProperty
    public ErrorLocation getErrorLocation()
    {
        return failureInfo.getErrorLocation();
    }

    @NotNull
    @JsonProperty
    public Optional<String> getTask()
    {
        return failureTask;
    }

    @NotNull
    @JsonProperty
    public Optional<URI> getUri()
    {
        return failureTaskUri;
    }

   @NotNull
    @JsonProperty
    public Optional<String> getHost()
    {
        return failureHost;
    }

    @NotNull
    @JsonProperty
    public Optional<Integer> getPort()
    {
        return failurePort;
    }

    @NotNull
    public FailureInfo getFailureInfo()
    {
        return failureInfo;
    }
}
