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
        assertQuery("SELECT iceberg.system.bucket(cast(10 as tinyint), 3)");
        assertQuery("SELECT iceberg.system.bucket(cast(0 as tinyint), 5)");
        assertQuery("SELECT iceberg.system.bucket(cast(1950 as smallint), 4)");
        assertQuery("SELECT iceberg.system.bucket(cast(0 as smallint), 7)");
        assertQuery("SELECT iceberg.system.bucket(cast(-1000 as smallint), 6)");
        assertQuery("SELECT iceberg.system.bucket(cast(-123456 as int), 8)");
        assertQuery("SELECT iceberg.system.bucket(cast(2147483647 as int), 20)");
        assertQuery("SELECT iceberg.system.bucket(cast(-2147483648 as int), 16)");
        assertQuery("SELECT iceberg.system.bucket(cast(2779099983928392323 as bigint), 6)");
        assertQuery("SELECT iceberg.system.bucket(cast(0 as bigint), 12)");
        assertQuery("SELECT iceberg.system.bucket(cast(-9876543210 as bigint), 9)");
        assertQuery("SELECT iceberg.system.bucket(cast(456.43 as DECIMAL(5,2)), 12)");
        assertQuery("SELECT iceberg.system.bucket(cast(0.00 as DECIMAL(5,2)), 8)");
        assertQuery("SELECT iceberg.system.bucket(cast(-99.99 as DECIMAL(5,2)), 10)");
        assertQuery("SELECT iceberg.system.bucket(cast(999.99 as DECIMAL(5,2)), 15)");
        assertQuery("SELECT iceberg.system.bucket(cast('12345678901234567890.1234567890' as DECIMAL(30,10)), 12)");
        assertQuery("SELECT iceberg.system.bucket(cast('0.0000000000' as DECIMAL(30,10)), 7)");
        assertQuery("SELECT iceberg.system.bucket(cast('-99999999999999999999.9999999999' as DECIMAL(30,10)), 20)");
        assertQuery("SELECT iceberg.system.bucket(cast('nasdbsdnsdms' as varchar), 7)");
        assertQuery("SELECT iceberg.system.bucket(cast('' as varchar), 4)");
        assertQuery("SELECT iceberg.system.bucket(cast('a' as varchar), 3)");
        assertQuery("SELECT iceberg.system.bucket(cast('123456789' as varchar), 8)");
        assertQuery("SELECT iceberg.system.bucket(cast('special@#$%chars' as varchar), 12)");
        assertQuery("SELECT iceberg.system.bucket(cast('binary data' as varbinary), 6)");
        assertQuery("SELECT iceberg.system.bucket(cast('' as varbinary), 5)");
        assertQuery("SELECT iceberg.system.bucket(cast('x' as varbinary), 4)");
        assertQuery("SELECT iceberg.system.bucket(cast('2018-04-06' as date), 9)");
        assertQuery("SELECT iceberg.system.bucket(cast('2023-01-01' as date), 12)");
        assertQuery("SELECT iceberg.system.bucket(cast('2000-12-31' as date), 7)");
        assertQuery("SELECT iceberg.system.bucket(CAST('2000-01-01 12:30:45.123' AS TIMESTAMP), 8)");
        assertQuery("SELECT iceberg.system.bucket(CAST('1970-01-01 00:00:00.000' AS TIMESTAMP), 6)");
        assertQuery("SELECT iceberg.system.bucket(CAST('2023-07-15 10:20:30.000 UTC' AS TIMESTAMP WITH TIME ZONE), 13)");
        assertQuery("SELECT iceberg.system.bucket(CAST('2000-06-15 18:45:12.000 GMT' AS TIMESTAMP WITH TIME ZONE), 9)");
    }

    @Test
    public void testBucketFunctionConsistency()
    {
        assertQuery("SELECT iceberg.system.bucket(cast(42 as bigint), 10) = iceberg.system.bucket(cast(42 as bigint), 10)");
        assertQuery("SELECT iceberg.system.bucket(cast('test' as varchar), 5) = iceberg.system.bucket(cast('test' as varchar), 5)");
        assertQuery("SELECT iceberg.system.bucket(cast('2023-01-01' as date), 7) = iceberg.system.bucket(cast('2023-01-01' as date), 7)");
    }
}
