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
package com.facebook.presto.thrift.api.udf;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum ThriftUdfErrorCodeSupplier
        implements ErrorCodeSupplier
{
    THRIFT_UDF_USER_ERROR(0, USER_ERROR),
    THRIFT_UDF_EXTERNAL_ERROR(1, EXTERNAL),
    THRIFT_UDF_SERVER_ERROR(2, INTERNAL_ERROR),
    THRIFT_UDF_INSUFFICIENT_RESOURCES(3, INSUFFICIENT_RESOURCES);

    private final ErrorCode errorCode;

    ThriftUdfErrorCodeSupplier(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0005_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
