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
package com.facebook.presto.google.sheets;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum SheetsErrorCode
        implements ErrorCodeSupplier
{
    SHEETS_BAD_CREDENTIALS_ERROR(0, EXTERNAL),
    SHEETS_METASTORE_ERROR(1, EXTERNAL),
    SHEETS_UNKNOWN_TABLE_ERROR(2, USER_ERROR),
    SHEETS_TABLE_LOAD_ERROR(3, INTERNAL_ERROR);

    private final ErrorCode errorCode;

    SheetsErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0508_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
