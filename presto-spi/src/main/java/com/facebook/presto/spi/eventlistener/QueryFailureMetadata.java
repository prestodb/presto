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

public class QueryFailureMetadata
{
    private final ErrorCode errorCode;
    private final String failureType;
    private final String failureMessage;
    private final String failureTask;
    private final String failureHost;
    private final String failuresJson;

    public QueryFailureMetadata(ErrorCode errorCode, String failureType, String failureMessage, String failureTask, String failureHost, String failuresJson)
    {
        this.errorCode = errorCode;
        this.failureType = failureType;
        this.failureMessage = failureMessage;
        this.failureTask = failureTask;
        this.failureHost = failureHost;
        this.failuresJson = failuresJson;
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public String getFailureType()
    {
        return failureType;
    }

    public String getFailureMessage()
    {
        return failureMessage;
    }

    public String getFailureTask()
    {
        return failureTask;
    }

    public String getFailureHost()
    {
        return failureHost;
    }

    public String getFailuresJson()
    {
        return failuresJson;
    }
}
