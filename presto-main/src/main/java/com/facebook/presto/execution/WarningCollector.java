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
import com.facebook.presto.client.QueryError;
import com.facebook.presto.spi.ErrorCode;

import java.util.List;

public interface WarningCollector
{
    void addWarning(QueryError warning);

    default void addWarning(ErrorCode warningErrorCode, ErrorLocation location, String message)
    {
        addWarning(new QueryError(
                message,
                null,
                warningErrorCode.getCode(),
                warningErrorCode.getName(),
                warningErrorCode.getType().toString(),
                location,
                null));
    }

    // May be taking copy of warning list (so, may be not O(1) time).
    List<QueryError> getWarnings();
}
