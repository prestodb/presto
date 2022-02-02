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
package com.facebook.presto.client;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
@ThriftStruct
public class QueryError
{
    private final String message;
    private final String sqlState;
    private final int errorCode;
    private final String errorName;
    private final String errorType;
    private final boolean retriable;
    private final ErrorLocation errorLocation;
    private final FailureInfo failureInfo;

    @JsonCreator
    @ThriftConstructor
    public QueryError(
            @JsonProperty("message") String message,
            @JsonProperty("sqlState") String sqlState,
            @JsonProperty("errorCode") int errorCode,
            @JsonProperty("errorName") String errorName,
            @JsonProperty("errorType") String errorType,
            @JsonProperty("boolean") boolean retriable,
            @JsonProperty("errorLocation") ErrorLocation errorLocation,
            @JsonProperty("failureInfo") FailureInfo failureInfo)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.errorName = errorName;
        this.errorType = errorType;
        this.retriable = retriable;
        this.errorLocation = errorLocation;
        this.failureInfo = failureInfo;
    }

    @JsonProperty
    @ThriftField(1)
    public String getMessage()
    {
        return message;
    }

    @Nullable
    @JsonProperty
    @ThriftField(2)
    public String getSqlState()
    {
        return sqlState;
    }

    @JsonProperty
    @ThriftField(3)
    public int getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    @ThriftField(4)
    public String getErrorName()
    {
        return errorName;
    }

    @JsonProperty
    @ThriftField(5)
    public String getErrorType()
    {
        return errorType;
    }

    @JsonProperty
    @ThriftField(6)
    public boolean isRetriable()
    {
        return retriable;
    }

    @Nullable
    @JsonProperty
    @ThriftField(7)
    public ErrorLocation getErrorLocation()
    {
        return errorLocation;
    }

    @Nullable
    @JsonProperty
    @ThriftField(8)
    public FailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("message", message)
                .add("sqlState", sqlState)
                .add("errorCode", errorCode)
                .add("errorName", errorName)
                .add("errorType", errorType)
                .add("retriable", retriable)
                .add("errorLocation", errorLocation)
                .add("failureInfo", failureInfo)
                .toString();
    }
}
