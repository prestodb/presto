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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

public enum ClickHouseErrorCode
        implements ErrorCodeSupplier
{
    JDBC_ERROR(0, EXTERNAL),
    JDBC_NON_TRANSIENT_ERROR(1, EXTERNAL),
    //TODO: Support more clickhouse error code
    CLICKHOUSE_METADATA_ERROR(2, EXTERNAL),
    CLICKHOUSE_DEEP_STORAGE_ERROR(3, EXTERNAL),
    CLICKHOUSE_SEGMENT_LOAD_ERROR(4, EXTERNAL),
    CLICKHOUSE_UNSUPPORTED_TYPE_ERROR(5, EXTERNAL),
    CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION(6, EXTERNAL),
    CLICKHOUSE_QUERY_GENERATOR_FAILURE(7, EXTERNAL),
    CLICKHOUSE_BROKER_RESULT_ERROR(8, EXTERNAL),
    CLICKHOUSE_AMBIGUOUS_OBJECT_NAME(9, EXTERNAL);

    private final ErrorCode errorCode;

    ClickHouseErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0400_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
