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
import com.facebook.presto.tests.AbstractTestCustomFunctions;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestDynamicFunctions
        extends AbstractTestCustomFunctions
{
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
            NativeQueryRunnerUtils.createOrders(javaQueryRunner);
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Parameters({"storageFormat", "sidecarEnabled"})
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        Path workingDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        Path prestoRoot = workingDir;
        // Traverse upward until finding the 'presto-native-execution' directory.
        while (prestoRoot != null && !Files.exists(prestoRoot.resolve("presto-native-execution"))) {
            prestoRoot = prestoRoot.getParent();
        }
        if (prestoRoot == null) {
            throw new IllegalStateException("Could not locate presto root directory.");
        }
        Path pluginDir = prestoRoot
                .resolve("presto-native-execution")
                .resolve("_build")
                .resolve("release")
                .resolve("presto_cpp")
                .resolve("main")
                .resolve("dynamic_registry")
                .resolve("examples");
        boolean sidecar = parseBoolean(System.getProperty("sidecarEnabled"));
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(System.getProperty("storageFormat"))
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecar)
                .setPluginDirectory(Optional.of(pluginDir.toString()))
                .build();
        if (sidecar) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    @Parameters("sidecarEnabled")
    @Test
    public void testCustomAdd()
    {
        if (parseBoolean(System.getProperty("sidecarEnabled"))) {
            assertQuery(
                    "SELECT custom_add(orderkey, custkey) FROM orders",
                    "SELECT orderkey + custkey FROM orders");
        }
    }

    // Aggregate functions are not supported currently.
    @Override
    @Test (enabled = false)
    public void testCustomSum()
    {
    }

    // Window functions are not supported currently.
    @Override
    @Test (enabled = false)
    public void testCustomRank()
    {
    }
}
