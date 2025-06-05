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

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestRepartitionQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Parameters;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestRepartitionQueriesWithSmallPages
        extends AbstractTestRepartitionQueries
{
    @Parameters({"storageFormat", "sidecarEnabled"})
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        boolean sidecar = parseBoolean(System.getProperty("sidecarEnabled"));
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(System.getProperty("storageFormat"))
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setExtraProperties(
                        // Use small SerializedPages to force flushing
                        ImmutableMap.of("driver.max-page-partitioning-buffer-size", "200B"))
                .setCoordinatorSidecarEnabled(sidecar)
                .build();
        if (sidecar) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Parameters("storageFormat")
    @Override
    protected void createTables()
    {
        try {
            String storageFormat = System.getProperty("storageFormat");
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                    .setStorageFormat(storageFormat)
                    .setAddStorageFormatToPath(true)
                    .build();
            if (storageFormat.equals("DWRF")) {
                NativeQueryRunnerUtils.createAllTables(javaQueryRunner, true);
            }
            else {
                NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            }
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
