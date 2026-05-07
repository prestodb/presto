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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestBinaryFunctions;
import org.testng.annotations.BeforeClass;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

/**
 * E2E test suite for binary functions including FNV hash functions.
 * This test runs against the native C++ implementation using PrestoNativeQueryRunner.
 *
 * The tests verify that the C++ implementation produces identical results to the Java
 * implementation for all FNV hash variants (fnv1-32, fnv1-64, fnv1a-32, fnv1a-64).
 */
public class TestBinaryFunctionsNative
        extends AbstractTestBinaryFunctions
{
    private String storageFormat;
    private boolean charNToVarcharImplicitCast;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        // Get configuration from system properties
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        charNToVarcharImplicitCast = parseBoolean(System.getProperty("charNToVarcharImplicitCast", "false"));

        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Create a native query runner that will execute queries using the C++ implementation
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setImplicitCastCharNToVarchar(charNToVarcharImplicitCast)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();

        // Setup native sidecar plugin if enabled
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }

        return queryRunner;
    }
}
