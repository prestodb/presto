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

import com.facebook.presto.scalar.sql.NativeSqlInvokedFunctionsPlugin;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory;
import com.facebook.presto.sidecar.typemanager.NativeTypeManagerFactory;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

public class NativeSidecarPluginQueryRunnerUtils
{
    private NativeSidecarPluginQueryRunnerUtils() {}

    public static void setupNativeSidecarPlugin(QueryRunner queryRunner)
    {
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        queryRunner.loadSessionPropertyProvider(
                NativeSystemSessionPropertyProviderFactory.NAME,
                ImmutableMap.of());
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));
        queryRunner.loadTypeManager(NativeTypeManagerFactory.NAME);
        queryRunner.loadPlanCheckerProviderManager("native", ImmutableMap.of());
        queryRunner.installPlugin(new NativeSqlInvokedFunctionsPlugin());
    }
}
