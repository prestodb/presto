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
package com.facebook.presto.iceberg.function;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.LiteralParameter;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.math.MathContext;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.SqlTimestamp.MICROSECONDS_PER_MILLISECOND;

public final class IcebergBucketFunction
{
    private IcebergBucketFunction() {}

    @ScalarFunction("bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long bucketInteger(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.LongType.get())
                .apply(value);
    }

    @ScalarFunction("bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long bucketVarchar(@SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
    {
        return (long) Transforms.bucket((int) numberOfBuckets)
                .bind(Types.StringType.get())
                .apply(value.toStringUtf8());
    }

    @ScalarFunction("bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long bucketVarbinary(@SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
    {
        return (long) Transforms.bucket((int) numberOfBuckets)
                .bind(Types.BinaryType.get())
                .apply(value.toByteBuffer());
    }

    @ScalarFunction("bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long bucketDate(@SqlType(StandardTypes.DATE) long value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.DateType.get())
                .apply((int) value);
    }

    @ScalarFunction("bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long bucketTimestamp(@SqlType(StandardTypes.TIMESTAMP) long value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withoutZone())
                .apply(value);
    }

    @ScalarFunction("bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long bucketTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withZone())
                .apply(unpackMillisUtc(value) * MICROSECONDS_PER_MILLISECOND);
    }

    @ScalarFunction("bucket")
    public static final class Bucket
    {
        @LiteralParameters({"p", "s"})
        @SqlType(StandardTypes.BIGINT)
        public static long bucketShortDecimal(@LiteralParameter("p") long numPrecision, @LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
        {
            return Transforms.bucket((int) numberOfBuckets)
                    .bind(Types.DecimalType.of((int) numPrecision, (int) numScale))
                    .apply(BigDecimal.valueOf(value));
        }

        @LiteralParameters({"p", "s"})
        @SqlType(StandardTypes.BIGINT)
        public static long bucketLongDecimal(@LiteralParameter("p") long numPrecision, @LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice value, @SqlType(StandardTypes.INTEGER) long numberOfBuckets)
        {
            return Transforms.bucket((int) numberOfBuckets)
                    .bind(Types.DecimalType.of((int) numPrecision, (int) numScale))
                    .apply(new BigDecimal(decodeUnscaledValue(value), (int) numScale, new MathContext((int) numPrecision)));
        }
    }
}
