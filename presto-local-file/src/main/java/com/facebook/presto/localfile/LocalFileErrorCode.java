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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum LocalFileErrorCode
        implements ErrorCodeSupplier
{
    LOCAL_FILE_NO_FILES(0, EXTERNAL),
    LOCAL_FILE_FILESYSTEM_ERROR(1, EXTERNAL),
    LOCAL_FILE_READ_ERROR(2, EXTERNAL);

    private final ErrorCode errorCode;

    LocalFileErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0501_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
