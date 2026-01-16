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

import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.nativeworker.AbstractTestNativeJmxMetadataMetrics;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

/**
 * Test class to verify JMX metadata metrics work correctly with Presto native (C++) workers using Iceberg connector.
 */
public class TestNativeIcebergJmxMetadataMetrics
        extends AbstractTestNativeJmxMetadataMetrics
{
    private IcebergQueryRunner icebergQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        icebergQueryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .setAddJmxPlugin(false)
                .buildIcebergQueryRunner();
        return icebergQueryRunner.getQueryRunner();
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

    @Override
    protected String getCatalogName()
    {
        return "iceberg";
    }

    @Override
    protected String getSchemaName()
    {
        return "tpch";
    }

    @Override
    protected MBeanServer getMBeanServer()
    {
        return ManagementFactory.getPlatformMBeanServer();
    }
}
