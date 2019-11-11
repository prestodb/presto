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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum MetastoreErrorCode
        implements ErrorCodeSupplier
{
    HIVE_METASTORE_ERROR(0, EXTERNAL),
    HIVE_TABLE_OFFLINE(2, USER_ERROR),
    HIVE_PARTITION_OFFLINE(6, USER_ERROR),
    HIVE_INVALID_PARTITION_VALUE(10, EXTERNAL),
    HIVE_INVALID_METADATA(12, EXTERNAL),
    HIVE_PATH_ALREADY_EXISTS(15, EXTERNAL),
    HIVE_FILESYSTEM_ERROR(16, EXTERNAL),
    HIVE_UNSUPPORTED_FORMAT(19, EXTERNAL),
    HIVE_TABLE_DROPPED_DURING_QUERY(35, EXTERNAL),
    HIVE_CORRUPTED_COLUMN_STATISTICS(37, EXTERNAL),
    HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE(39, INTERNAL_ERROR),
    /* Shared error code with HiveErrorCode */;

    private final ErrorCode errorCode;

    public static final int ERROR_CODE_MASK = 0x0100_0000;

    MetastoreErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
