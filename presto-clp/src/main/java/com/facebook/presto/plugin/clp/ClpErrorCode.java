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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

public enum ClpErrorCode
        implements ErrorCodeSupplier
{
    CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION(0, EXTERNAL),
    CLP_UNSUPPORTED_METADATA_SOURCE(1, EXTERNAL),
    CLP_UNSUPPORTED_SPLIT_SOURCE(2, EXTERNAL),
    CLP_UNSUPPORTED_TYPE(3, EXTERNAL),
    CLP_UNSUPPORTED_CONFIG_OPTION(4, EXTERNAL);

    private final ErrorCode errorCode;

    ClpErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0400_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
