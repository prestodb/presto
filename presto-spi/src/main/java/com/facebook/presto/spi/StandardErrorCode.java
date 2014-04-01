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
{
    USER_ERROR(0x0000_0000),
    SYNTAX_ERROR(0x0000_0001),
    ABANDONED_QUERY(0x0000_0002),
    USER_CANCELED(0x0000_0003),
    CANNOT_DROP_TABLE(0x0000_0004),
    NOT_FOUND(0x0000_0005),
    FUNCTION_NOT_FOUND(0x0000_0006),
    INVALID_FUNCTION_ARGUMENT(0x0000_0007),
    DIVISION_BY_ZERO(0x0000_0008),
    INVALID_CAST_ARGUMENT(0x0000_0009),

    INTERNAL(0x0001_0000),
    TOO_MANY_REQUESTS_FAILED(0x0001_0001),

    INSUFFICIENT_RESOURCES(0x0002_0000),
    EXCEEDED_MEMORY_LIMIT(0x0002_0001),

    // Connectors can use error codes starting at EXTERNAL
    EXTERNAL(0x0100_0000);

    private final ErrorCode errorCode;

    StandardErrorCode(int code)
    {
        errorCode = new ErrorCode(code, name());
    }

    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    public static ErrorType toErrorType(int code)
    {
        if (code < INTERNAL.toErrorCode().getCode()) {
            return ErrorType.USER_ERROR;
        }
        if (code < INSUFFICIENT_RESOURCES.toErrorCode().getCode()) {
            return ErrorType.INTERNAL;
        }
        if (code < EXTERNAL.toErrorCode().getCode()) {
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
