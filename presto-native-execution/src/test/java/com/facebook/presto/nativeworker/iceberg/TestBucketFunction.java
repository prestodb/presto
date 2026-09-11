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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

public class TestBucketFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Test
    public void testBucketFunction()
    {
        assertQuery("SELECT iceberg.system.bucket(3, cast(10 as tinyint))");
        assertQuery("SELECT iceberg.system.bucket(5, cast(0 as tinyint))");
        assertQuery("SELECT iceberg.system.bucket(4, cast(1950 as smallint))");
        assertQuery("SELECT iceberg.system.bucket(7, cast(0 as smallint))");
        assertQuery("SELECT iceberg.system.bucket(6, cast(-1000 as smallint))");
        assertQuery("SELECT iceberg.system.bucket(8, cast(-123456 as int))");
        assertQuery("SELECT iceberg.system.bucket(20, cast(2147483647 as int))");
        assertQuery("SELECT iceberg.system.bucket(16, cast(-2147483648 as int))");
        assertQuery("SELECT iceberg.system.bucket(6, cast(2779099983928392323 as bigint))");
        assertQuery("SELECT iceberg.system.bucket(12, cast(0 as bigint))");
        assertQuery("SELECT iceberg.system.bucket(9, cast(-9876543210 as bigint))");
        assertQuery("SELECT iceberg.system.bucket(12, cast(456.43 as DECIMAL(5,2)))");
        assertQuery("SELECT iceberg.system.bucket(8, cast(0.00 as DECIMAL(5,2)))");
        assertQuery("SELECT iceberg.system.bucket(10, cast(-99.99 as DECIMAL(5,2)))");
        assertQuery("SELECT iceberg.system.bucket(15, cast(999.99 as DECIMAL(5,2)))");
        assertQuery("SELECT iceberg.system.bucket(12, cast('12345678901234567890.1234567890' as DECIMAL(30,10)))");
        assertQuery("SELECT iceberg.system.bucket(7, cast('0.0000000000' as DECIMAL(30,10)))");
        assertQuery("SELECT iceberg.system.bucket(20, cast('-99999999999999999999.9999999999' as DECIMAL(30,10)))");
        assertQuery("SELECT iceberg.system.bucket(7, cast('nasdbsdnsdms' as varchar))");
        assertQuery("SELECT iceberg.system.bucket(4, cast('' as varchar))");
        assertQuery("SELECT iceberg.system.bucket(3, cast('a' as varchar))");
        assertQuery("SELECT iceberg.system.bucket(8, cast('123456789' as varchar))");
        assertQuery("SELECT iceberg.system.bucket(12, cast('special@#$%chars' as varchar))");
        assertQuery("SELECT iceberg.system.bucket(6, cast('binary data' as varbinary))");
        assertQuery("SELECT iceberg.system.bucket(5, cast('' as varbinary))");
        assertQuery("SELECT iceberg.system.bucket(4, cast('x' as varbinary))");
        assertQuery("SELECT iceberg.system.bucket(9, cast('2018-04-06' as date))");
        assertQuery("SELECT iceberg.system.bucket(12, cast('2023-01-01' as date))");
        assertQuery("SELECT iceberg.system.bucket(7, cast('2000-12-31' as date))");
        assertQuery("SELECT iceberg.system.bucket(8, CAST('2000-01-01 12:30:45.123' AS TIMESTAMP))");
        assertQuery("SELECT iceberg.system.bucket(6, CAST('1970-01-01 00:00:00.000' AS TIMESTAMP))");
        assertQuery("SELECT iceberg.system.bucket(12, CAST('2023-07-15 10:20:30.000 UTC' AS TIMESTAMP WITH TIME ZONE))");
        assertQuery("SELECT iceberg.system.bucket(9, CAST('2000-06-15 18:45:12.000 GMT' AS TIMESTAMP WITH TIME ZONE))");
    }

    @Test
    public void testBucketFunctionConsistency()
    {
        assertQuery("SELECT iceberg.system.bucket(10, cast(42 as bigint)) = iceberg.system.bucket(10, cast(42 as bigint))");
        assertQuery("SELECT iceberg.system.bucket(5, cast('test' as varchar)) = iceberg.system.bucket(5, cast('test' as varchar))");
        assertQuery("SELECT iceberg.system.bucket(7, cast('2023-01-01' as date)) = iceberg.system.bucket(7, cast('2023-01-01' as date))");
    }

    @Test
    public void testBucketFunctionWithTables()
    {
        assertQuery("SELECT iceberg.system.bucket(16, value) FROM test_bucket_int");
        assertQuery("SELECT iceberg.system.bucket(4, amount) FROM test_bucket_mixed");
        assertQuery("SELECT iceberg.system.bucket(12, created_date) FROM test_bucket_mixed");
        assertQuery("SELECT iceberg.system.bucket(10, category) FROM test_bucket_groupby");
        assertQuery("SELECT iceberg.system.bucket(8, amount) FROM test_bucket_decimal");
        assertQuery("SELECT iceberg.system.bucket(9, event_date) FROM test_bucket_date");
        assertQuery("SELECT iceberg.system.bucket(12, ts) FROM test_bucket_timestamp");
    }
}
