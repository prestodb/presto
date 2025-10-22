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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.iceberg.function.IcebergBucketFunction;
import com.facebook.presto.metadata.FunctionExtractor;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.Bucket.bucketLongDecimal;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.Bucket.bucketShortDecimal;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketDate;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketInteger;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketTimestamp;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketTimestampWithTimeZone;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketVarbinary;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketVarchar;
import static io.airlift.slice.Slices.utf8Slice;

public class TestIcebergScalarFunctions
        extends AbstractTestFunctions
{
    public TestIcebergScalarFunctions()
    {
        super(TEST_SESSION, new FeaturesConfig(), new FunctionsConfig(), false);
    }

    @BeforeClass
    public void registerFunction()
    {
        ImmutableList.Builder<Class<?>> functions = ImmutableList.builder();
        functions.add(IcebergBucketFunction.class)
                .add(IcebergBucketFunction.Bucket.class);
        functionAssertions.addConnectorFunctions(FunctionExtractor.extractFunctions(functions.build(),
                new CatalogSchemaName("iceberg", "system")), "iceberg");
    }

    @Test
    public void testBucketFunction()
    {
        String catalogSchema = "iceberg.system";
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(10 as tinyint), 3)", BIGINT, bucketInteger(10, 3));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(1950 as smallint), 4)", BIGINT, bucketInteger(1950, 4));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(2375645 as int), 5)", BIGINT, bucketInteger(2375645, 5));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(2779099983928392323 as bigint), 6)", BIGINT, bucketInteger(2779099983928392323L, 6));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(456.43 as DECIMAL(5,2)), 12)", BIGINT, bucketShortDecimal(5, 2, 45643, 12));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('12345678901234567890.1234567890' as DECIMAL(30,10)), 12)", BIGINT, bucketLongDecimal(30, 10, encodeScaledValue(new BigDecimal("12345678901234567890.1234567890")), 12));

        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('nasdbsdnsdms' as varchar), 7)", BIGINT, bucketVarchar(utf8Slice("nasdbsdnsdms"), 7));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('nasdbsdnsdms' as varbinary), 8)", BIGINT, bucketVarbinary(utf8Slice("nasdbsdnsdms"), 8));

        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('2018-04-06' as date), 9)", BIGINT, bucketDate(DateOperators.castFromSlice(utf8Slice("2018-04-06")), 9));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2018-04-06 04:35:00.000' AS TIMESTAMP),10)", BIGINT, bucketTimestamp(TimestampOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2018-04-06 04:35:00.000")), 10));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2018-04-06 04:35:00.000 GMT' AS TIMESTAMP WITH TIME ZONE), 11)", BIGINT, bucketTimestampWithTimeZone(TimestampWithTimeZoneOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2018-04-06 04:35:00.000 GMT")), 11));
    }
}
