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
package com.facebook.presto.spark;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;

public enum SparkErrorCode
        implements ErrorCodeSupplier
{
    GENERIC_SPARK_ERROR(0, INTERNAL_ERROR),
    SPARK_EXECUTOR_OOM(1, INTERNAL_ERROR),
    SPARK_EXECUTOR_LOST(2, INTERNAL_ERROR),
    EXCEEDED_SPARK_DRIVER_MAX_RESULT_SIZE(3, INSUFFICIENT_RESOURCES),
    UNSUPPORTED_STORAGE_TYPE(4, INTERNAL_ERROR),
    STORAGE_ERROR(5, EXTERNAL),
    MALFORMED_QUERY_FILE(6, EXTERNAL)
    /**/;

    private final ErrorCode errorCode;

    public static final int ERROR_CODE_MASK = 0x0003_0000;

    SparkErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
