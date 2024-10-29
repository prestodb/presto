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
package com.facebook.presto.sidecar.nativechecker;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;

public enum NativePlanCheckerErrorCode
        implements ErrorCodeSupplier
{
    NATIVEPLANCHECKER_CONNECTION_ERROR(0, EXTERNAL),
    NATIVEPLANCHECKER_UNKNOWN_CONVERSION_FAILURE(1, INTERNAL_ERROR),
    NATIVEPLANCHECKER_RESPONSE_MISSING_BODY(2, INTERNAL_ERROR);

    private final ErrorCode errorCode;

    NativePlanCheckerErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0519_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
