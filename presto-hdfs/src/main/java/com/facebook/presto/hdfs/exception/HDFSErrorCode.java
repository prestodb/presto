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
package com.facebook.presto.hdfs.exception;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public enum HDFSErrorCode
    implements ErrorCodeSupplier
{
    HDFS_RECORD_MORE_LESS_ERROR(0, EXTERNAL),
    COL_TYPE_NONVALID(1, EXTERNAL),
    TYPE_UNKNOWN(2, EXTERNAL),
    ARRAY_LENGTH_NOT_MATCH(3, EXTERNAL),
    TABLE_NOT_FOUND(4, EXTERNAL),
    HDFS_SPLIT_NOT_OPEN(5, EXTERNAL),
    HDFS_CURSOR_ERROR(6, EXTERNAL),
    META_CURRUPTION(7, EXTERNAL),
    FUNCTION_UNSUPPORTED(8, EXTERNAL),
    COLUMN_NOT_FOUND(9, EXTERNAL);

    private final ErrorCode errorCode;

    HDFSErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0210_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
