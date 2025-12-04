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
import com.google.common.collect.ImmutableList;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class NativeTestsUtils
{
    private NativeTestsUtils() {}

    public static QueryRunner createNativeQueryRunner(String storageFormat, boolean charNToVarcharImplicitCast, boolean sidecarEnabled, boolean customFunctionsEnabled)
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setImplicitCastCharNToVarchar(charNToVarcharImplicitCast)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setPluginDirectory(customFunctionsEnabled ? Optional.of(getCustomFunctionsPluginDirectory().toString()) : Optional.empty())
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    public static QueryRunner createNativeQueryRunner(String storageFormat, boolean charNToVarcharImplicitCast, boolean sidecarEnabled)
            throws Exception
    {
        return createNativeQueryRunner(storageFormat, false, sidecarEnabled, false);
    }

    public static QueryRunner createNativeQueryRunner(String storageFormat, boolean sidecarEnabled)
            throws Exception
    {
        return createNativeQueryRunner(storageFormat, false, sidecarEnabled);
    }

    public static void createTables(String storageFormat)
    {
        try {
            QueryRunner javaQueryRunner = javaHiveQueryRunnerBuilder()
                    .setStorageFormat(storageFormat)
                    .setAddStorageFormatToPath(true)
                    .build();
            if (storageFormat.equals("DWRF")) {
                NativeQueryRunnerUtils.createAllTables(javaQueryRunner, true, true);
            }
            else {
                NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false, true);
            }
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: remove and directly return charNToVarcharImplicitCast after addressing Issue #25894 and adding support for Char(n) type
    // to class NativeTypeManager for Sidecar.
    public static boolean getCharNToVarcharImplicitCastForTest(boolean sidecarEnabled, boolean charNToVarcharImplicitCast)
    {
        return sidecarEnabled ? false : charNToVarcharImplicitCast;
    }

    public static Path getCustomFunctionsPluginDirectory()
            throws Exception
    {
        Path prestoRoot = findPrestoRoot();

        // All candidate paths relative to prestoRoot
        List<Path> candidates = ImmutableList.of(
                prestoRoot.resolve("presto-native-tests/_build/debug/presto_cpp/tests/custom_functions"),
                prestoRoot.resolve("presto-native-tests/_build/release/presto_cpp/tests/custom_functions"));

        return candidates.stream()
                .filter(Files::exists)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Could not locate custom functions directory"));
    }

    private static Path findPrestoRoot()
    {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            if (Files.exists(dir.resolve("presto-native-tests")) ||
                    Files.exists(dir.resolve("presto-native-execution"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Could not locate presto root directory.");
    }
}
