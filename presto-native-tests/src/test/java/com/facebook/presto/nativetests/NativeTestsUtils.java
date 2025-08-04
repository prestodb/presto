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
import com.facebook.presto.testing.QueryRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class NativeTestsUtils
{
    private NativeTestsUtils() {}

    public static QueryRunner createNativeQueryRunner(String storageFormat, boolean sidecarEnabled)
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setPluginDirectory(sidecarEnabled ? Optional.of(getCustomFunctionsPluginDirectory().toString()) : Optional.empty())
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    public static void createTables(String storageFormat)
    {
        try {
            QueryRunner javaQueryRunner = javaHiveQueryRunnerBuilder()
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

    public static Path getCustomFunctionsPluginDirectory()
            throws Exception
    {
        Path workingDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        Path prestoRoot = workingDir;
        // Traverse upward until finding the 'presto-native-tests' directory.
        while (prestoRoot != null && !Files.exists(prestoRoot.resolve("presto-native-tests"))) {
            prestoRoot = prestoRoot.getParent();
        }
        if (prestoRoot == null) {
            throw new IllegalStateException("Could not locate presto root directory.");
        }
        Path buildPath = prestoRoot
                .resolve("presto-native-tests")
                .resolve("_build");
        if (Files.exists(buildPath.resolve("debug"))) {
            buildPath = buildPath.resolve("debug");
        }
        else if (Files.exists(buildPath.resolve("release"))) {
            buildPath = buildPath.resolve("release");
        }
        else {
            throw new IllegalStateException("presto-native-tests build path does not exist");
        }
        return buildPath
                .resolve("presto_cpp")
                .resolve("tests")
                .resolve("custom_functions");
    }
}
