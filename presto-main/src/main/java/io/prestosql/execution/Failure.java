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
package io.prestosql.execution;

import io.prestosql.spi.ErrorCode;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class Failure
        extends RuntimeException
{
    private final String type;
    private final ErrorCode errorCode;

    Failure(String type, String message, @Nullable ErrorCode errorCode, Failure cause)
    {
        super(message, cause);
        this.type = requireNonNull(type, "type is null");
        this.errorCode = errorCode;
    }

    public String getType()
    {
        return type;
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String toString()
    {
        String message = getMessage();
        if (message != null) {
            return type + ": " + message;
        }
        return type;
    }
}
