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
package com.facebook.presto.druid.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import org.apache.druid.segment.ColumnValueSelector;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.druid.DruidErrorCode.DRUID_UNSUPPORTED_TYPE_ERROR;
import static java.lang.String.format;

public interface ColumnReader
{
    Block readBlock(Type type, int batchSize);

    static ColumnReader createColumnReader(Type type, ColumnValueSelector valueSelector)
    {
        if (type == VARCHAR) {
            return new StringColumnReader(valueSelector);
        }
        if (type == DOUBLE) {
            return new DoubleColumnReader(valueSelector);
        }
        if (type == BIGINT) {
            return new LongColumnReader(valueSelector);
        }
        if (type == REAL) {
            return new FloatColumnReader(valueSelector);
        }
        if (type == TIMESTAMP) {
            return new TimestampColumnReader(valueSelector);
        }
        throw new PrestoException(DRUID_UNSUPPORTED_TYPE_ERROR, format("Unsupported type: %s", type));
    }
}
