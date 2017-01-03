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
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum HiveErrorCode
        implements ErrorCodeSupplier
{
    HIVE_METASTORE_ERROR(0, EXTERNAL),
    HIVE_CURSOR_ERROR(1, EXTERNAL),
    HIVE_TABLE_OFFLINE(2, EXTERNAL),
    HIVE_CANNOT_OPEN_SPLIT(3, EXTERNAL),
    HIVE_FILE_NOT_FOUND(4, EXTERNAL),
    HIVE_UNKNOWN_ERROR(5, EXTERNAL),
    HIVE_PARTITION_OFFLINE(6, EXTERNAL),
    HIVE_BAD_DATA(7, EXTERNAL),
    HIVE_PARTITION_SCHEMA_MISMATCH(8, EXTERNAL),
    HIVE_MISSING_DATA(9, EXTERNAL),
    HIVE_INVALID_PARTITION_VALUE(10, EXTERNAL),
    HIVE_TIMEZONE_MISMATCH(11, EXTERNAL),
    HIVE_INVALID_METADATA(12, EXTERNAL),
    HIVE_INVALID_VIEW_DATA(13, EXTERNAL),
    HIVE_DATABASE_LOCATION_ERROR(14, EXTERNAL),
    HIVE_PATH_ALREADY_EXISTS(15, EXTERNAL),
    HIVE_FILESYSTEM_ERROR(16, EXTERNAL),
    // code HIVE_WRITER_ERROR(17) is deprecated
    HIVE_SERDE_NOT_FOUND(18, EXTERNAL),
    HIVE_UNSUPPORTED_FORMAT(19, EXTERNAL),
    HIVE_PARTITION_READ_ONLY(20, EXTERNAL),
    HIVE_TOO_MANY_OPEN_PARTITIONS(21, EXTERNAL),
    HIVE_CONCURRENT_MODIFICATION_DETECTED(22, EXTERNAL),
    HIVE_COLUMN_ORDER_MISMATCH(23, USER_ERROR),
    HIVE_FILE_MISSING_COLUMN_NAMES(24, EXTERNAL),
    HIVE_WRITER_OPEN_ERROR(25, EXTERNAL),
    HIVE_WRITER_CLOSE_ERROR(26, EXTERNAL),
    HIVE_WRITER_DATA_ERROR(27, EXTERNAL),
    HIVE_INVALID_BUCKET_FILES(28, EXTERNAL),
    HIVE_EXCEEDED_PARTITION_LIMIT(29, USER_ERROR);

    private final ErrorCode errorCode;

    HiveErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
