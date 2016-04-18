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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;

public enum JdbcErrorCode
        implements ErrorCodeSupplier
{
    JDBC_ERROR(0),
    JDBC_NON_TRANSIENT_ERROR(1);

    private final ErrorCode errorCode;

    JdbcErrorCode(int code)
    {
        errorCode = new ErrorCode(code + 0x0400_0000, name());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
