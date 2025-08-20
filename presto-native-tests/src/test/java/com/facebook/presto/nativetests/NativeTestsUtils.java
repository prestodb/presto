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

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class NativeTestsUtils
{
    private NativeTestsUtils() {}

    public static QueryRunner createNativeQueryRunner(String storageFormat, boolean charNToVarcharImplicitCast, boolean sidecarEnabled)
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setImplicitCastCharNToVarchar(charNToVarcharImplicitCast)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
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

    // TODO: remove and directly return charNToVarcharImplicitCast after addressing Issue #25894 and adding support for Char(n) type
    // to class NativeTypeManager for Sidecar.
    public static boolean getCharNToVarcharImplicitCastForTest(boolean sidecarEnabled, boolean charNToVarcharImplicitCast)
    {
        return sidecarEnabled ? false : charNToVarcharImplicitCast;
    }
}
