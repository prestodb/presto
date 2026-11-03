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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.USER_ERROR;

public enum DuckLakeErrorCode
        implements ErrorCodeSupplier
{
    DUCKLAKE_CATALOG_ERROR(0, EXTERNAL),
    DUCKLAKE_INVALID_METADATA(1, EXTERNAL),
    DUCKLAKE_UNSUPPORTED_TYPE(2, USER_ERROR),
    DUCKLAKE_UNSUPPORTED_FEATURE(3, USER_ERROR),
    DUCKLAKE_MISSING_DATA(4, EXTERNAL),
    DUCKLAKE_BAD_DELETE_FILE(5, EXTERNAL),
    DUCKLAKE_BAD_DATA(6, EXTERNAL),
    DUCKLAKE_INVALID_SNAPSHOT(7, USER_ERROR),
    /**/;

    // Error code range 0x0521_0000-0x0521_FFFF is reserved for the DuckLake connector. See
    // presto-iceberg, presto-delta, presto-lance and other connector ErrorCode classes for the
    // set of ranges already in use.
    private final ErrorCode errorCode;

    DuckLakeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0521_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
