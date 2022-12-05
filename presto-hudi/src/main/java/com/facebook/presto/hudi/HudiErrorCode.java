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

package com.facebook.presto.hudi;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;

public enum HudiErrorCode
        implements ErrorCodeSupplier
{
    HUDI_UNKNOWN_TABLE_TYPE(0x00, INTERNAL_ERROR),
    HUDI_INVALID_METADATA(0x01, EXTERNAL),
    HUDI_INVALID_PARTITION_VALUE(0x02, EXTERNAL),
    HUDI_FILESYSTEM_ERROR(0x40, EXTERNAL),
    HUDI_CANNOT_OPEN_SPLIT(0x41, EXTERNAL),
    HUDI_CURSOR_ERROR(0x42, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    HudiErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0505_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
