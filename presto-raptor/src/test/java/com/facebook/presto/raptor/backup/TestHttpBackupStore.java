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
package com.facebook.presto.raptor.backup;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import javax.inject.Singleton;

import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.io.Files.createTempDir;
import static com.google.inject.util.Modules.override;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.testing.FileUtils.deleteRecursively;

public class TestHttpBackupStore
        extends AbstractTestBackupStore<BackupStore>
{
    private LifeCycleManager lifeCycleManager;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        temporary = createTempDir();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("backup.http.uri", "http://localhost:8080")
                .build();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                binder -> jaxrsBinder(binder).bind(TestingHttpBackupResource.class),
                override(new HttpBackupModule()).with(new TestingModule()));

        Injector injector = app
                .strictConfig()
                .setRequiredConfigurationProperties(properties)
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        store = injector.getInstance(BackupStore.class);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        deleteRecursively(temporary);
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    private static class TestingModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForHttpBackup
        public Supplier<URI> createBackupUriSupplier(HttpServerInfo serverInfo)
        {
            return serverInfo::getHttpUri;
        }
    }
}
