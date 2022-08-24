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
package com.facebook.presto.parquet;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;

public enum ParquetErrorCode
        implements ErrorCodeSupplier
{
    PARQUET_UNSUPPORTED_COLUMN_TYPE(0, INTERNAL_ERROR),
    PARQUET_UNSUPPORTED_ENCODING(1, INTERNAL_ERROR),
    PARQUET_IO_READ_ERROR(2, EXTERNAL),
    PARQUET_INCORRECT_DECODING(3, INTERNAL_ERROR),
    PARQUET_BAD_DATA(4, EXTERNAL),
    PARQUET_MISSING_DATA(5, EXTERNAL),
    PARQUET_CANNOT_OPEN_SPLIT(6, EXTERNAL);
    private final ErrorCode errorCode;

    ParquetErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0605_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
