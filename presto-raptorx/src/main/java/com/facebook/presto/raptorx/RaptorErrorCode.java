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
package com.facebook.presto.raptorx;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;

public enum RaptorErrorCode
        implements ErrorCodeSupplier
{
    RAPTOR_INTERNAL_ERROR(0, INTERNAL_ERROR),
    RAPTOR_METADATA_ERROR(1, EXTERNAL),
    RAPTOR_CORRUPT_METADATA(2, EXTERNAL),
    RAPTOR_STORAGE_ERROR(3, EXTERNAL),
    RAPTOR_RECOVERY_ERROR(4, EXTERNAL),
    RAPTOR_RECOVERY_TIMEOUT(5, EXTERNAL),
    RAPTOR_CHUNK_STORE_ERROR(6, EXTERNAL),
    RAPTOR_CHUNK_STORE_TIMEOUT(7, EXTERNAL),
    RAPTOR_CHUNK_STORE_CORRUPTION(8, EXTERNAL),
    RAPTOR_CHUNK_NOT_FOUND(9, EXTERNAL),
    RAPTOR_LOCAL_DISK_FULL(10, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    RaptorErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0301_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
