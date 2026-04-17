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

import com.facebook.presto.common.AuthClientConfigs;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.presto.server.CommonInternalCommunicationModule.bindInternalAuth;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public class NativeSidecarCommunicationModule
        implements Module
{
    private static final MBeanExporter EXPORTER = new SidecarAwareMBeanExporter(getPlatformMBeanServer());
    private final AuthClientConfigs authClientConfigs;

    public NativeSidecarCommunicationModule(AuthClientConfigs authClientConfigs)
    {
        this.authClientConfigs = requireNonNull(authClientConfigs, "authClientConfigs is null");
    }

    @Override
    public void configure(Binder binder)
    {
        bindInternalAuth(binder, authClientConfigs);
        httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);
    }

    public static void installMBeanModule(Binder binder)
    {
        Module module = Modules.override(new MBeanModule())
                .with(new AbstractModule()
                {
                    @Override
                    protected void configure()
                    {
                        bind(MBeanExporter.class).toInstance(EXPORTER);
                    }
                });
        binder.install(module);
        binder.bind(MBeanServer.class).toInstance(getPlatformMBeanServer());
    }

    /**
     * Custom MBeanExporter to avoid duplicate JMX registration for the sidecar HttpClient.
     * Multiple modules bind the same HttpClient ("sidecar", @ForSidecarInfo) across different injectors,
     * but JMX is JVM-global and requires unique ObjectNames. This exporter ensures the sidecar MBean is
     * registered only once while preserving default behavior for all other MBeans.
     **/
    private static class SidecarAwareMBeanExporter
            extends MBeanExporter
    {
        private static final String SIDECAR_BEAN_OBJECT_NAME =
                "com.facebook.airlift.http.client:type=HttpClient,name=ForSidecarInfo";
        private final Set<String> exportedBeanNames = ConcurrentHashMap.newKeySet();

        public SidecarAwareMBeanExporter(MBeanServer server)
        {
            super(server);
        }

        @Override
        public void export(String name, Object object)
        {
            if (SIDECAR_BEAN_OBJECT_NAME.equals(name) && !exportedBeanNames.add(name)) {
                return;
            }

            try {
                super.export(name, object);
            }
            catch (RuntimeException e) {
                // Only suppress duplicate for @ForSidecarInfo
                if (SIDECAR_BEAN_OBJECT_NAME.equals(name) && isInstanceAlreadyExists(e)) {
                    return;
                }
                throw e;
            }
        }

        private boolean isInstanceAlreadyExists(Throwable t)
        {
            while (t != null) {
                if (t instanceof InstanceAlreadyExistsException) {
                    return true;
                }
                t = t.getCause();
            }
            return false;
        }
    }
}
