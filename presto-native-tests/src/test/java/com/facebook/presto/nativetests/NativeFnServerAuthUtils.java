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
import com.facebook.presto.server.TestingFunctionServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.utils.FnServerAuthTestUtils;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static java.lang.String.format;

/**
 * Native-worker factory methods for Function Server authentication tests.
 *
 * <p>These methods live in {@code presto-native-tests} rather than
 * {@link FnServerAuthTestUtils} (which lives in {@code presto-function-server})
 * because they depend on {@link PrestoNativeQueryRunnerUtils} and
 * {@link NativeQueryRunnerUtils} from {@code presto-native-execution}, and on
 * {@link FnServerAuthTestUtils} from {@code presto-function-server}.
 * {@code presto-native-tests} already depends on both modules, making it the
 * correct home. This avoids adding a {@code presto-function-server →
 * presto-native-execution} or reverse test-jar dependency.
 */
public final class NativeFnServerAuthUtils
{
    private NativeFnServerAuthUtils() {}

    /** Java coordinator + native (C++) workers, mTLS + JWT. */
    public static DistributedQueryRunner createNativeRunnerWithMtlsAndJwt()
            throws Exception
    {
        return createNativeRunnerWithMtlsAndJwt(true);
    }

    /** Java coordinator + native (C++) workers, mTLS + JWT, with configurable sidecar. */
    public static DistributedQueryRunner createNativeRunnerWithMtlsAndJwt(boolean sidecarEnabled)
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                FnServerAuthTestUtils.getFunctionServerConfigWithAuth(
                        FnServerAuthTestUtils.findUnusedPort(), FnServerAuthTestUtils.JWT_SHARED_SECRET, true),
                true,
                sidecarEnabled);
    }

    /** Java coordinator + native workers, mTLS only (no JWT on any node). */
    public static DistributedQueryRunner createNativeRunnerWithOnlyMtls()
            throws Exception
    {
        return createNativeRunnerWithOnlyMtls(true);
    }

    /** Java coordinator + native workers, mTLS only, with configurable sidecar. */
    public static DistributedQueryRunner createNativeRunnerWithOnlyMtls(boolean sidecarEnabled)
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                FnServerAuthTestUtils.getFunctionServerConfigWithAuth(
                        FnServerAuthTestUtils.findUnusedPort(), FnServerAuthTestUtils.JWT_SHARED_SECRET, false),
                false,
                sidecarEnabled);
    }

    /**
     * Returns an external worker launcher for a native (C++) worker configured with
     * HTTPS client certificates, optionally JWT, and configurable sidecar.
     */
    public static Optional<BiFunction<Integer, URI, Process>> getHttpsNativeWorkerLauncher(
            String prestoServerPath,
            String functionServerUri,
            boolean includeJwt,
            boolean sidecarEnabled)
    {
        String workerCertPath = FnServerAuthTestUtils.certPath("worker/worker.crt");
        String workerKeyPath = FnServerAuthTestUtils.certPath("worker/worker.key");
        String workerCombinedPemPath = FnServerAuthTestUtils.certPath("worker/worker-combined.pem");
        String caCertPath = FnServerAuthTestUtils.certPath("ca/ca.crt");
        if (!Files.exists(Paths.get(workerCombinedPemPath))) {
            throw new IllegalStateException(
                    "Worker combined PEM file not found at: " + workerCombinedPemPath);
        }
        PrestoNativeQueryRunnerUtils.HttpsClientConfig httpsConfig = PrestoNativeQueryRunnerUtils.HttpsClientConfig.of(
                workerCertPath, workerKeyPath, workerCombinedPemPath, caCertPath);
        if (includeJwt) {
            httpsConfig = httpsConfig.withJwt(FnServerAuthTestUtils.JWT_SHARED_SECRET);
        }
        return PrestoNativeQueryRunnerUtils.externalWorkerLauncherBuilder()
                .setPrestoServerPath(prestoServerPath)
                .setCatalogName("tpch")
                .setConnectorName("tpch")
                .setRemoteFunctionServerRestUrl(functionServerUri)
                .setEnableRuntimeMetricsCollection(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setHttpsClientConfig(httpsConfig)
                .build();
    }

    private static DistributedQueryRunner createHttpsNativeQueryRunnerWithFnServer(
            Map<String, String> functionServerConfig,
            boolean includeJwt,
            boolean sidecarEnabled)
            throws Exception
    {
        Path prestoServerPath = Paths.get(System.getProperty("PRESTO_SERVER",
                "_build/debug/presto_cpp/main/presto_server")).toAbsolutePath();
        if (!Files.exists(prestoServerPath)) {
            throw new IllegalStateException(format(
                    "Native worker binary at %s not found. " +
                            "Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.",
                    prestoServerPath));
        }
        TestingFunctionServer functionServer = FnServerAuthTestUtils.startFunctionServer(functionServerConfig);
        DistributedQueryRunner queryRunner = null;
        try {
            String functionServerUri = FnServerAuthTestUtils.convertToLocalhostUri(functionServer.getServerUri());
            Map<String, String> coordinatorProperties = sidecarEnabled
                    ? ImmutableMap.<String, String>builder()
                            .putAll(FnServerAuthTestUtils.buildCoordinatorProperties(includeJwt))
                            .putAll(NativeQueryRunnerUtils.getNativeSidecarProperties())
                            .build()
                    : FnServerAuthTestUtils.buildCoordinatorProperties(includeJwt);
            queryRunner = DistributedQueryRunner.builder(FnServerAuthTestUtils.defaultTpchSession())
                    .setNodeCount(1)
                    .setExtraProperties(NativeQueryRunnerUtils.getNativeWorkerSystemProperties())
                    .setCoordinatorProperties(coordinatorProperties)
                    .setExternalWorkerLauncher(
                            getHttpsNativeWorkerLauncher(prestoServerPath.toString(), functionServerUri, includeJwt, sidecarEnabled))
                    .build();
            FnServerAuthTestUtils.setupTpchAndFunctionNamespace(queryRunner, functionServerUri);
            queryRunner.addCloseAction(functionServer);
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, functionServer);
            throw e;
        }
    }
}
