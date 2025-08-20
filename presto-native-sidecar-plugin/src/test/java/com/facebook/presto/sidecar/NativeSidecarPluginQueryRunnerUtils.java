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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.sidecar.expressions.NativeExpressionOptimizerFactory;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory;
import com.facebook.presto.sidecar.typemanager.NativeTypeManagerFactory;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static java.lang.String.format;

public class NativeSidecarPluginQueryRunnerUtils
{
    private static final Logger log = Logger.get(NativeSidecarPluginQueryRunnerUtils.class);

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
        queryRunner.getExpressionManager().loadExpressionOptimizerFactory(NativeExpressionOptimizerFactory.NAME, "native", ImmutableMap.of());
    }

    public static int findRandomPortForSidecar()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static Process getNativeSidecarProcess(URI discoveryUri, int port)
            throws IOException
    {
        PrestoNativeQueryRunnerUtils.NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        return getNativeSidecarProcess(nativeQueryRunnerParameters.serverBinary.toString(), discoveryUri, port);
    }

    private static Process getNativeSidecarProcess(String prestoServerPath, URI discoveryUri, int port)
            throws IOException
    {
        Path tempDirectoryPath = Files.createTempDirectory(PrestoNativeQueryRunnerUtils.class.getSimpleName());
        log.info("Temp directory for Sidecar: %s", tempDirectoryPath.toString());

        // Write config file
        String configProperties = format("discovery.uri=%s%n" +
                "presto.version=testversion%n" +
                "system-memory-gb=4%n" +
                "native-sidecar=true%n" +
                "http-server.http.port=%d", discoveryUri, port);

        Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());
        Files.write(tempDirectoryPath.resolve("node.properties"),
                format("node.id=%s%n" +
                        "node.internal-address=127.0.0.1%n" +
                        "node.environment=testing%n" +
                        "node.location=test-location", UUID.randomUUID()).getBytes());

        // TODO: sidecars require that a catalog directory exist
        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
        Files.createDirectory(catalogDirectoryPath);

        return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                .directory(tempDirectoryPath.toFile())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("sidecar.out").toFile()))
                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("sidecar.out").toFile()))
                .start();
    }
}
