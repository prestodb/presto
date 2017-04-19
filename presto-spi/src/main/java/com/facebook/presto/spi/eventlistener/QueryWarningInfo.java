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

public class QueryWarningInfo
{
    private final ErrorCode errorCode;
    private final String message;
    private final Optional<QueryErrorLocation> location;

    public QueryWarningInfo(
            ErrorCode errorCode,
            String message,
            Optional<QueryErrorLocation> location)
    {
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.message = requireNonNull(message, "message is null");
        this.location = requireNonNull(location, "location is null");
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public String getMessage()
    {
        return message;
    }

    public Optional<QueryErrorLocation> getLocation()
    {
        return location;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(errorCode.toString());
        builder.append(": ");
        if (location.isPresent()) {
            builder.append(location.get().toString()).append(": ");
        }
        builder.append(message);
        return builder.toString();
    }
}
