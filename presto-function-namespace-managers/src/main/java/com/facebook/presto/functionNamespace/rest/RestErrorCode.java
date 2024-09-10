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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

public enum RestErrorCode
        implements ErrorCodeSupplier
{
    REST_SERVER_NOT_FOUND(0, EXTERNAL),
    REST_SERVER_ERROR(1, EXTERNAL),
    REST_SERVER_TIMEOUT(2, EXTERNAL),
    REST_SERVER_CONNECT_ERROR(3, EXTERNAL),
    REST_SERVER_BAD_RESPONSE(4, EXTERNAL),
    REST_SERVER_IO_ERROR(5, EXTERNAL),
    REST_SERVER_FUNCTION_FETCH_ERROR(6, EXTERNAL);

    private final ErrorCode errorCode;

    public static final int ERROR_CODE_MASK = 0x0002_1000;

    RestErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
