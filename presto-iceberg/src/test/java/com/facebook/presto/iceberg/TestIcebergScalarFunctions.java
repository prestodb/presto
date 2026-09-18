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
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
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
        functionAssertions.assertFunction(catalogSchema + ".bucket(3, cast(10 as tinyint))", INTEGER, (int) (bucketInteger(3, 10)));
        functionAssertions.assertFunction(catalogSchema + ".bucket(4, cast(1950 as smallint))", INTEGER, (int) bucketInteger(4, 1950));
        functionAssertions.assertFunction(catalogSchema + ".bucket(5, cast(2375645 as int))", INTEGER, (int) bucketInteger(5, 2375645));
        functionAssertions.assertFunction(catalogSchema + ".bucket(6, cast(2779099983928392323 as BIGINT))", INTEGER, (int) bucketInteger(6, 2779099983928392323L));
        functionAssertions.assertFunction(catalogSchema + ".bucket(12, cast(456.43 as DECIMAL(5,2)))", INTEGER, (int) bucketShortDecimal(5, 2, 12, 45643));
        functionAssertions.assertFunction(catalogSchema + ".bucket(12, cast('12345678901234567890.1234567890' as DECIMAL(30,10)))", INTEGER, (int) bucketLongDecimal(30, 10, 12, encodeScaledValue(new BigDecimal("12345678901234567890.1234567890"))));

        functionAssertions.assertFunction(catalogSchema + ".bucket(7, cast('nasdbsdnsdms' as varchar))", INTEGER, (int) bucketVarchar(7, utf8Slice("nasdbsdnsdms")));
        functionAssertions.assertFunction(catalogSchema + ".bucket(8, cast('nasdbsdnsdms' as varbinary))", INTEGER, (int) bucketVarbinary(8, utf8Slice("nasdbsdnsdms")));

        functionAssertions.assertFunction(catalogSchema + ".bucket(9, cast('2018-04-06' as date))", INTEGER, (int) bucketDate(9, DateOperators.castFromSlice(utf8Slice("2018-04-06"))));
        functionAssertions.assertFunction(catalogSchema + ".bucket(10, CAST('2018-04-06 04:35:00.000' AS TIMESTAMP))", INTEGER, (int) bucketTimestamp(10, TimestampOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2018-04-06 04:35:00.000"))));
        functionAssertions.assertFunction(catalogSchema + ".bucket(11, CAST('2018-04-06 04:35:00.000 GMT' AS TIMESTAMP WITH TIME ZONE))", INTEGER, (int) bucketTimestampWithTimeZone(11, TimestampWithTimeZoneOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2018-04-06 04:35:00.000 GMT"))));
    }
}
