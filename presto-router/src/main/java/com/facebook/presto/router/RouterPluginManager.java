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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.PluginInstaller;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.PluginManagerUtil;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RouterPlugin;
import com.facebook.presto.spi.router.SchedulerFactory;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.google.common.collect.ImmutableList;
import io.airlift.resolver.ArtifactResolver;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.server.PluginManagerUtil.SPI_PACKAGES;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class RouterPluginManager
{
    private static final String SERVICES_FILE = "META-INF/services/" + Plugin.class.getName();

    private static final Logger log = Logger.get(RouterPluginManager.class);

    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final ArtifactResolver resolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final PluginInstaller pluginInstaller;
    private final PrestoAuthenticatorManager prestoAuthenticatorManager;

    List<SchedulerFactory> registeredSchedulerFactoryList = new ArrayList<>();

    public List<SchedulerFactory> getRegisteredSchedulerFactoryList()
    {
        return registeredSchedulerFactoryList;
    }

    @Inject
    public RouterPluginManager(
            PluginManagerConfig config,
            PasswordAuthenticatorManager passwordAuthenticatorManager,
            PrestoAuthenticatorManager prestoAuthenticatorManager)
    {
        requireNonNull(config, "config is null");

        this.installedPluginsDir = config.getInstalledPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.pluginInstaller = new RouterPluginInstaller(this);
        this.prestoAuthenticatorManager = requireNonNull(prestoAuthenticatorManager, "prestoAuthenticatorManager is null");
    }

    public void loadPlugins()
            throws Exception
    {
        PluginManagerUtil.loadPlugins(
                pluginsLoading,
                pluginsLoaded,
                installedPluginsDir,
                plugins,
                null,
                resolver,
                SPI_PACKAGES,
                null,
                SERVICES_FILE,
                pluginInstaller,
                getClass().getClassLoader());
    }

    public void installPlugin(Plugin plugin)
    {
        for (PasswordAuthenticatorFactory authenticatorFactory : plugin.getPasswordAuthenticatorFactories()) {
            log.info("Registering password authenticator %s", authenticatorFactory.getName());
            passwordAuthenticatorManager.addPasswordAuthenticatorFactory(authenticatorFactory);
        }

        for (PrestoAuthenticatorFactory authenticatorFactory : plugin.getPrestoAuthenticatorFactories()) {
            log.info("Registering presto authenticator %s", authenticatorFactory.getName());
            prestoAuthenticatorManager.addPrestoAuthenticatorFactory(authenticatorFactory);
        }
    }

    public void installRouterPlugin(RouterPlugin plugin)
    {
        for (SchedulerFactory schedulerFactory : plugin.getSchedulerFactories()) {
            log.info("Registering router scheduler  %s", schedulerFactory.getName());
            registeredSchedulerFactoryList.add(schedulerFactory);
        }
    }

    private static class RouterPluginInstaller
            implements PluginInstaller
    {
        private final RouterPluginManager pluginManager;

        public RouterPluginInstaller(RouterPluginManager pluginManager)
        {
            this.pluginManager = pluginManager;
        }

        @Override
        public void installPlugin(Plugin plugin)
        {
            pluginManager.installPlugin(plugin);
        }

        @Override
        public void installCoordinatorPlugin(CoordinatorPlugin plugin)
        {
            throw new UnsupportedOperationException("Cannot install coordinator plugins on router");
        }

        @Override
        public void installRouterPlugin(RouterPlugin plugin)
        {
            pluginManager.installRouterPlugin(plugin);
        }
    }
}
