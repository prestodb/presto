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

public enum HiveErrorCode
        implements ErrorCodeSupplier
{
    // Connectors can use error codes starting at EXTERNAL
    HIVE_METASTORE_ERROR(0x0100_0000),
    HIVE_CURSOR_ERROR(0x0100_0001),
    HIVE_TABLE_OFFLINE(0x0100_0002),
    HIVE_CANNOT_OPEN_SPLIT(0x0100_0003),
    HIVE_FILE_NOT_FOUND(0x0100_0004),
    HIVE_UNKNOWN_ERROR(0x0100_0005),
    HIVE_PARTITION_OFFLINE(0x0100_0006),
    HIVE_BAD_DATA(0x0100_0007),
    HIVE_PARTITION_SCHEMA_MISMATCH(0x0100_0008),
    HIVE_MISSING_DATA(0x0100_0009),
    HIVE_INVALID_PARTITION_VALUE(0x0100_000A),
    HIVE_TIMEZONE_MISMATCH(0x0100_000B),
    HIVE_INVALID_METADATA(0x0100_000C),
    HIVE_INVALID_VIEW_DATA(0x0100_000D),
    HIVE_DATABASE_LOCATION_ERROR(0x0100_000E),
    HIVE_PATH_ALREADY_EXISTS(0x0100_000F),
    HIVE_FILESYSTEM_ERROR(0x0100_0010),
//  code HIVE_WRITER_ERROR(0x0100_0011) is deprecated
    HIVE_SERDE_NOT_FOUND(0x0100_0012),
    HIVE_UNSUPPORTED_FORMAT(0x0100_0013),
    HIVE_PARTITION_READ_ONLY(0x0100_00014),
    HIVE_TOO_MANY_OPEN_PARTITIONS(0x0100_0015),
    HIVE_CONCURRENT_MODIFICATION_DETECTED(0x0100_0016),
    HIVE_COLUMN_ORDER_MISMATCH(0x0100_0017),
    HIVE_FILE_MISSING_COLUMN_NAMES(0x0100_0018),
    HIVE_WRITER_OPEN_ERROR(0x0100_0019),
    HIVE_WRITER_CLOSE_ERROR(0x0100_001A),
    HIVE_WRITER_DATA_ERROR(0x0100_001B);

    private final ErrorCode errorCode;

    HiveErrorCode(int code)
    {
        errorCode = new ErrorCode(code, name());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
