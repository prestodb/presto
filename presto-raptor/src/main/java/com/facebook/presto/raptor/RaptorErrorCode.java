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
package com.facebook.presto.raptor;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorCodeSupplier;
import com.facebook.presto.common.ErrorType;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

public enum RaptorErrorCode
        implements ErrorCodeSupplier
{
    RAPTOR_ERROR(0, EXTERNAL),
    RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS(1, EXTERNAL),
    RAPTOR_NO_HOST_FOR_SHARD(2, EXTERNAL),
    RAPTOR_RECOVERY_ERROR(3, EXTERNAL),
    RAPTOR_BACKUP_TIMEOUT(4, EXTERNAL),
    RAPTOR_METADATA_ERROR(5, EXTERNAL),
    RAPTOR_BACKUP_ERROR(6, EXTERNAL),
    RAPTOR_BACKUP_NOT_FOUND(7, EXTERNAL),
    RAPTOR_REASSIGNMENT_DELAY(8, EXTERNAL),
    RAPTOR_REASSIGNMENT_THROTTLE(9, EXTERNAL),
    RAPTOR_RECOVERY_TIMEOUT(10, EXTERNAL),
    RAPTOR_CORRUPT_METADATA(11, EXTERNAL),
    RAPTOR_LOCAL_DISK_FULL(12, EXTERNAL),
    RAPTOR_BACKUP_CORRUPTION(13, EXTERNAL),
    RAPTOR_NOT_ENOUGH_NODES(14, EXTERNAL),
    RAPTOR_WRITER_DATA_ERROR(15, EXTERNAL),
    RAPTOR_UNSUPPORTED_COMPRESSION_KIND(16, EXTERNAL),
    RAPTOR_FILE_SYSTEM_ERROR(17, EXTERNAL),
    RAPTOR_TOO_MANY_FILES_CREATED(18, EXTERNAL);

    private final ErrorCode errorCode;

    RaptorErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0300_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
