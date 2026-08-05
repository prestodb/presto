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
import static com.facebook.presto.common.ErrorType.INSUFFICIENT_RESOURCES;
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
    ARROW_FLIGHT_INVALID_CERT_ERROR(6, INTERNAL_ERROR),
    // Raised by the native Arrow Flight client; mirrored in
    // presto-native-execution/presto_cpp/main/common/Exception.h
    ARROW_FLIGHT_REMOTE_ERROR(7, EXTERNAL),
    ARROW_FLIGHT_UNAVAILABLE_ERROR(8, EXTERNAL, true),
    ARROW_FLIGHT_AUTH_ERROR(9, EXTERNAL),
    ARROW_FLIGHT_INTERNAL_ERROR(10, INTERNAL_ERROR),
    ARROW_FLIGHT_RESOURCE_ERROR(11, INSUFFICIENT_RESOURCES);

    private final ErrorCode errorCode;

    ArrowErrorCode(int code, ErrorType type)
    {
        this(code, type, false);
    }

    ArrowErrorCode(int code, ErrorType type, boolean retriable)
    {
        errorCode = new ErrorCode(code + 0x0510_0000, name(), type, retriable);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
