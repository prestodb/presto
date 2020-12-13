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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum PinotErrorCode
        implements ErrorCodeSupplier
{
    PINOT_UNSUPPORTED_COLUMN_TYPE(0, EXTERNAL), // schema issues
    PINOT_QUERY_GENERATOR_FAILURE(1, INTERNAL_ERROR), // Accepted a query whose pql we couldn't generate
    PINOT_INSUFFICIENT_SERVER_RESPONSE(2, EXTERNAL, true), // numServersResponded < numServersQueried
    PINOT_EXCEPTION(3, EXTERNAL), // Exception reported by pinot
    PINOT_HTTP_ERROR(4, EXTERNAL), // Some non okay http error code
    PINOT_UNEXPECTED_RESPONSE(5, EXTERNAL), // Invalid json response with okay http return code
    PINOT_UNSUPPORTED_EXPRESSION(6, INTERNAL_ERROR), // Unsupported function
    PINOT_UNABLE_TO_FIND_BROKER(7, EXTERNAL),
    PINOT_DECODE_ERROR(8, EXTERNAL),
    PINOT_INVALID_PQL_GENERATED(9, INTERNAL_ERROR),
    PINOT_INVALID_CONFIGURATION(10, INTERNAL_ERROR),
    PINOT_DATA_FETCH_EXCEPTION(11, EXTERNAL, true),
    PINOT_REQUEST_GENERATOR_FAILURE(12, INTERNAL_ERROR),
    PINOT_UNABLE_TO_FIND_INSTANCE(13, EXTERNAL),
    PINOT_INVALID_SEGMENT_QUERY_GENERATED(14, INTERNAL_ERROR),
    PINOT_PUSH_DOWN_QUERY_NOT_PRESENT(20, USER_ERROR),
    PINOT_UNCLASSIFIED_ERROR(100, EXTERNAL);

    /**
     * Connectors can use error codes starting at the range 0x0100_0000
     * See https://github.com/prestodb/presto/wiki/Error-Codes
     *
     * @see com.facebook.presto.spi.StandardErrorCode
     */

    private final ErrorCode errorCode;
    private final boolean retriable;

    PinotErrorCode(int code, ErrorType type, boolean retriable)
    {
        errorCode = new ErrorCode(code + 0x0505_0000, name(), type);
        this.retriable = retriable;
    }

    PinotErrorCode(int code, ErrorType type)
    {
        this(code, type, false);
    }

    public boolean isRetriable()
    {
        return retriable;
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
