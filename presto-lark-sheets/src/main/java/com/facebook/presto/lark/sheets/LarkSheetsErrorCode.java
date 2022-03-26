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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum LarkSheetsErrorCode
        implements ErrorCodeSupplier
{
    // general errors
    NOT_PERMITTED(0x01, USER_ERROR),
    LARK_API_ERROR(0x02, EXTERNAL),
    // schema errors
    SCHEMA_TOKEN_NOT_PROVIDED(0x10, USER_ERROR),
    SCHEMA_ALREADY_EXISTS(0x11, USER_ERROR),
    SCHEMA_NOT_EXISTS(0x12, USER_ERROR),
    SCHEMA_NOT_READABLE(0x13, USER_ERROR),
    // sheet errors
    SHEET_NAME_AMBIGUOUS(0x40, USER_ERROR),
    SHEET_INVALID_HEADER(0x41, EXTERNAL),
    SHEET_BAD_DATA(0x42, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    LarkSheetsErrorCode(int code, ErrorType errorType)
    {
        this.errorCode = new ErrorCode(code + 0x0518_0000, name(), errorType);
    }

    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
