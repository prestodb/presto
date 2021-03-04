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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum BigQueryErrorCode
        implements ErrorCodeSupplier
{
    BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED(0, EXTERNAL),
    BIGQUERY_FAILED_TO_EXECUTE_QUERY(1, EXTERNAL),
    BIGQUERY_QUERY_FAILED_UNKNOWN(2, EXTERNAL),
    BIGQUERY_UNSUPPORTED_TYPE_FOR_SLICE(3, EXTERNAL),
    BIGQUERY_UNSUPPORTED_COLUMN_TYPE(4, EXTERNAL),
    BIGQUERY_UNSUPPORTED_TYPE_FOR_BLOCK(5, EXTERNAL),
    BIGQUERY_UNSUPPORTED_TYPE_FOR_LONG(6, EXTERNAL),
    BIGQUERY_UNSUPPORTED_TYPE_FOR_VARBINARY(7, EXTERNAL),
    BIGQUERY_TABLE_DISAPPEAR_DURING_LIST(8, EXTERNAL),
    BIGQUERY_ERROR_END_OF_AVRO_BUFFER(9, EXTERNAL),
    BIGQUERY_ERROR_READING_NEXT_AVRO_RECORD(10, EXTERNAL);

    private final ErrorCode errorCode;

    BigQueryErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0509_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
