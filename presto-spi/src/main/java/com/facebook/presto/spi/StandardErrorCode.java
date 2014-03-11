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
package com.facebook.presto.spi;

public enum StandardErrorCode
        implements ErrorCode
{
    USER_ERROR(0x0000_0000),
    SYNTAX_ERROR(0x0000_0001),
    ABANDONED_QUERY(0x0000_0002),
    USER_CANCELED(0x0000_0003),
    CANNOT_DROP_TABLE(0x0000_0004),
    NOT_FOUND(0x0000_0005),
    FUNCTION_NOT_FOUND(0x0000_0006),

    INTERNAL(0x0001_0000),
    TOO_MANY_REQUESTS_FAILED(0x0001_0001),

    INSUFFICIENT_RESOURCES(0x0002_0000),
    EXCEEDED_MEMORY_LIMIT(0x0002_0001),

    // Connectors can use error codes starting at EXTERNAL
    EXTERNAL(0x0100_0000);

    private final int code;

    StandardErrorCode(int code)
    {
        this.code = code;
    }

    @Override
    public int getCode()
    {
        return code;
    }

    @Override
    public String getName()
    {
        return name();
    }

    public static ErrorType toErrorType(int code)
    {
        if (code < INTERNAL.getCode()) {
            return ErrorType.USER_ERROR;
        }
        if (code < INSUFFICIENT_RESOURCES.getCode()) {
            return ErrorType.INTERNAL;
        }
        if (code < EXTERNAL.getCode()) {
            return ErrorType.INSUFFICIENT_RESOURCES;
        }
        return ErrorType.EXTERNAL;
    }

    public enum ErrorType
    {
        USER_ERROR,
        INTERNAL,
        INSUFFICIENT_RESOURCES,
        EXTERNAL
    }
}
