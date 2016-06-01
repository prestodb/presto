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

import com.facebook.presto.spi.ErrorCode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryFailureInfo
{
    private final ErrorCode errorCode;
    private final Optional<String> failureType;
    private final Optional<String> failureMessage;
    private final Optional<String> failureTask;
    private final Optional<String> failureHost;
    private final String failuresJson;

    public QueryFailureInfo(
            ErrorCode errorCode,
            Optional<String> failureType,
            Optional<String> failureMessage,
            Optional<String> failureTask,
            Optional<String> failureHost,
            String failuresJson)
    {
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.failureType = requireNonNull(failureType, "failureType is null");
        this.failureMessage = requireNonNull(failureMessage, "failureMessage is null");
        this.failureTask = requireNonNull(failureTask, "failureTask is null");
        this.failureHost = requireNonNull(failureHost, "failureHost is null");
        this.failuresJson = requireNonNull(failuresJson, "failuresJson is null");
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public Optional<String> getFailureType()
    {
        return failureType;
    }

    public Optional<String> getFailureMessage()
    {
        return failureMessage;
    }

    public Optional<String> getFailureTask()
    {
        return failureTask;
    }

    public Optional<String> getFailureHost()
    {
        return failureHost;
    }

    public String getFailuresJson()
    {
        return failuresJson;
    }
}
