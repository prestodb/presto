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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class QueryError
{
    private final String message;
    private final String sqlState;
    private final int errorCode;
    private final String errorName;
    private final String errorType;
    private final ErrorLocation errorLocation;
    private final FailureInfo failureInfo;

    @JsonCreator
    public QueryError(
            @JsonProperty("message") String message,
            @JsonProperty("sqlState") String sqlState,
            @JsonProperty("errorCode") int errorCode,
            @JsonProperty("errorName") String errorName,
            @JsonProperty("errorType") String errorType,
            @JsonProperty("errorLocation") ErrorLocation errorLocation,
            @JsonProperty("failureInfo") FailureInfo failureInfo)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.errorName = errorName;
        this.errorType = errorType;
        this.errorLocation = errorLocation;
        this.failureInfo = failureInfo;
    }

    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    @Nullable
    @JsonProperty
    public String getSqlState()
    {
        return sqlState;
    }

    @JsonProperty
    public int getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    public String getErrorName()
    {
        return errorName;
    }

    @JsonProperty
    public String getErrorType()
    {
        return errorType;
    }

    @Nullable
    @JsonProperty
    public ErrorLocation getErrorLocation()
    {
        return errorLocation;
    }

    @Nullable
    @JsonProperty
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
                .add("errorLocation", errorLocation)
                .add("failureInfo", failureInfo)
                .toString();
    }
}
