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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;

public enum RaptorErrorCode
        implements ErrorCodeSupplier
{
    RAPTOR_ERROR(0x0300_0000),
    RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS(0x0300_0001),
    RAPTOR_NO_HOST_FOR_SHARD(0x0300_0002),
    RAPTOR_RECOVERY_ERROR(0x0300_0003),
    RAPTOR_BACKUP_TIMEOUT(0x0300_0004),
    RAPTOR_METADATA_ERROR(0x0300_0005),
    RAPTOR_BACKUP_ERROR(0x0300_0006),
    RAPTOR_BACKUP_NOT_FOUND(0x300_0007);

    private final ErrorCode errorCode;

    RaptorErrorCode(int code)
    {
        errorCode = new ErrorCode(code, name());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
