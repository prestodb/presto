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
package com.facebook.presto.opentelemetry;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

public enum OpenTelemetryErrorCode
        implements ErrorCodeSupplier
{
    OPEN_TELEMETRY_CONTEXT_PROPAGATOR_ERROR(0, EXTERNAL);

    private final ErrorCode errorCode;

    OpenTelemetryErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0507_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
