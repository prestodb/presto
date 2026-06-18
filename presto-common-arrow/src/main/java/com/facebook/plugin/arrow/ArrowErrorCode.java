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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;

public enum ArrowErrorCode
        implements ErrorCodeSupplier
{
    ARROW_FLIGHT_INFO_ERROR(0, EXTERNAL),
    ARROW_INTERNAL_ERROR(1, INTERNAL_ERROR),
    ARROW_FLIGHT_CLIENT_ERROR(2, EXTERNAL),
    ARROW_FLIGHT_METADATA_ERROR(3, EXTERNAL),
    ARROW_FLIGHT_TYPE_ERROR(4, EXTERNAL),
    ARROW_FLIGHT_INVALID_KEY_ERROR(5, INTERNAL_ERROR),
    ARROW_FLIGHT_INVALID_CERT_ERROR(6, INTERNAL_ERROR);

    private final ErrorCode errorCode;

    ArrowErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0510_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
