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
package com.facebook.presto.sidecar;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public class TestNativeSidecarNotSetForCatalog
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(false)
                .build();
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testHiveCatalogFailure()
    {
        assertQueryFails("SELECT hive.default.initcap('Hello world')", "(?s).*Function hive.default.initcap not registered*");
    }

    @Test
    public void testNativeCatalogFailure()
    {
        assertQueryFails("SELECT native.default.array_sum(ARRAY[1, 2, 3])", "(?s).*Function native.default.array_sum not registered*");
    }

    @Test
    public void testDefaultCatalog()
    {
        assertQuery("SELECT array_sum(ARRAY[1, 2, 3])");
    }

    @Test
    public void testCustomCatalogFailure()
    {
        assertQueryFails("SELECT mycatalog.myschema.custom_func('test')", "(?s).*Function mycatalog.myschema.custom_func not registered*");
    }
}
