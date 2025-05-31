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
package com.facebook.presto.router;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.password.file.FileAuthenticatorFactory;
import com.facebook.presto.router.security.RouterSecurityModule;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.inject.Injector;
import jakarta.ws.rs.core.UriBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Optional;

import static com.facebook.presto.router.TestingRouterUtil.createPrestoServer;
import static com.facebook.presto.router.TestingRouterUtil.getConfigFile;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestRouterAuthentication
{
    private static final Logger log = Logger.get(TestRouterAuthentication.class);
    private static final String jksPassword = "testPass";

    private File configFile;
    private LifeCycleManager lifeCycleManager;
    private Path authenticatorPropertiesFile;
    private HttpServerInfo httpServerInfo;
    private Path storePath;
    private Path keystorePath;
    private Path truststorePath;
    private HttpClient httpClient;

    private void runKeytoolCommand(String command, Object... args)
            throws IOException, InterruptedException
    {
        String cmd = format(command, args);
        Process process = new ProcessBuilder()
                .directory(storePath.toFile())
                .command(Splitter.on(" ").splitToStream(cmd).toArray(String[]::new))
                .start();
        process.waitFor();
        if (log.isDebugEnabled()) {
            log.debug("running command: " + cmd + ". Output:\n" + new String(ByteStreams.toByteArray(process.getInputStream())));
        }
    }

    private void setupCertStores()
            throws Exception
    {
        runKeytoolCommand("keytool -genkeypair -alias localhost -keyalg RSA -keysize 2048 -validity 365 -keystore keystore.jks -storepass %s -keypass %s -dname CN=localhost -ext SAN=ip:127.0.0.1", jksPassword, jksPassword);
        runKeytoolCommand("keytool -exportcert -alias localhost -keystore keystore.jks -file certificate.cer -storepass %s", jksPassword);
        runKeytoolCommand("keytool -importcert -alias localhost -file certificate.cer -keystore truststore.jks -storepass %s -noprompt", jksPassword);
        keystorePath = storePath.resolve("keystore.jks");
        truststorePath = storePath.resolve("truststore.jks");
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        Path tempFile = Files.createTempFile("temp-config", ".json");
        configFile = getConfigFile(ImmutableList.of(createPrestoServer()), tempFile.toFile());

        authenticatorPropertiesFile = Paths.get("etc/password-authenticator.properties");
        authenticatorPropertiesFile.getParent().toFile().mkdirs();

        Path passwordFilePath = Files.createTempFile("passwords", ".db");
        // credentials come from htpasswd -n -B -C 8, input "testpass"
        Files.write(passwordFilePath, "testuser:$2y$08$KBfSimK6KTZFyCKlJACpTu7VMBHlnFixXm8tDh9I0rDf3IIuobtHy".getBytes(UTF_8));
        Files.write(
                authenticatorPropertiesFile,
                format("password-authenticator.name=file\nfile.password-file=%s", passwordFilePath).getBytes(UTF_8),
                StandardOpenOption.CREATE);
        storePath = Files.createTempDirectory("jks-store");
        setupCertStores();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new RouterSecurityModule(),
                new RouterModule(Optional.empty()));

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .setRequiredConfigurationProperty("presto.version", "test")
                .setOptionalConfigurationProperty("http-server.authentication.type", "PASSWORD")
                .setOptionalConfigurationProperty("http-server.http.enabled", "false")
                .setOptionalConfigurationProperty("http-server.https.enabled", "true")
                .setOptionalConfigurationProperty("http-server.https.keystore.path", keystorePath.toAbsolutePath().toString())
                .setOptionalConfigurationProperty("http-server.https.keystore.key", jksPassword)
                .initialize();
        injector.getInstance(RouterPluginManager.class).loadPlugins();

        PasswordAuthenticatorManager passwordAuthenticatorManager = injector.getInstance(PasswordAuthenticatorManager.class);
        PasswordAuthenticatorFactory authFactory = new FileAuthenticatorFactory();
        passwordAuthenticatorManager.addPasswordAuthenticatorFactory(authFactory);
        passwordAuthenticatorManager.setRequired();
        passwordAuthenticatorManager.loadPasswordAuthenticator();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        httpServerInfo = injector.getInstance(HttpServerInfo.class);
        httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setTrustStorePath(truststorePath.toAbsolutePath().toString())
                        .setTrustStorePassword(jksPassword));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
    {
        if (authenticatorPropertiesFile != null) {
            try {
                authenticatorPropertiesFile.toFile().delete();
            }
            catch (Exception e) {
                // ignore
            }
        }
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    @DataProvider(name = "routerEndpoints")
    public Object[][] routerEndpoints()
    {
        return new Object[][] {
                // method, endpoint, expected response with auth
                {"GET", "/", 200},
                {"GET", "/v1/info", 200},
                {"GET", "/v1/cluster", 200},
                // 400 because the test doesn't actually POST a request body
                {"POST", "/v1/statement", 400},
                // TODO: The UI is intentionally left out because the airlift incorrectly configures
                //  some static resources
                // {"GET", "/ui/"}
        };
    }

    @Test(dataProvider = "routerEndpoints")
    public void testEndpointWithoutAuthentication(String method, String endpoint, int unused)
            throws Exception
    {
        Request request = Request.builder()
                .setUri(UriBuilder.fromUri(httpServerInfo.getHttpsUri()).path(endpoint).build())
                .setMethod(method)
                .build();
        httpClient.execute(request, new ResponseHandler<Void, Exception>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
                    throws Exception
            {
                throw exception;
            }

            @Override
            public Void handle(Request request, Response response)
                    throws Exception
            {
                assertEquals(response.getStatusCode(), 401, format("request to %s should not have been successful. Expected 401, got: %d%n%s",
                        request.getUri(), response.getStatusCode(), new String(ByteStreams.toByteArray(response.getInputStream()))));
                return null;
            }
        });
    }

    @Test(dataProvider = "routerEndpoints")
    public void testEndpointWithAuthentication(String method, String endpoint, int expectedCode)
            throws Exception
    {
        Request request = Request.builder()
                .setUri(UriBuilder.fromUri(httpServerInfo.getHttpsUri()).path(endpoint).build())
                .setMethod(method)
                .setHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString("testuser:testpass".getBytes(UTF_8)))
                .build();
        httpClient.execute(request, new ResponseHandler<Void, Exception>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
                    throws Exception
            {
                throw exception;
            }

            @Override
            public Void handle(Request request, Response response)
                    throws Exception
            {
                assertEquals(response.getStatusCode(), expectedCode, format("request to %s should have been successful. Expected %d, got: %d%n%s",
                        request.getUri(), expectedCode, response.getStatusCode(), new String(ByteStreams.toByteArray(response.getInputStream()))));
                return null;
            }
        });
    }
}
