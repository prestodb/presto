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
package com.facebook.presto.druid;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum DruidErrorCode
        implements ErrorCodeSupplier
{
    DRUID_METADATA_ERROR(0, EXTERNAL),
    DRUID_DEEP_STORAGE_ERROR(1, EXTERNAL),
    DRUID_SEGMENT_LOAD_ERROR(2, EXTERNAL),
    DRUID_UNSUPPORTED_TYPE_ERROR(3, EXTERNAL),
    DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION(4, EXTERNAL),
    DRUID_QUERY_GENERATOR_FAILURE(5, EXTERNAL),
    DRUID_BROKER_RESULT_ERROR(6, EXTERNAL),
    DRUID_AMBIGUOUS_OBJECT_NAME(7, EXTERNAL);

    private final ErrorCode errorCode;

    DruidErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
