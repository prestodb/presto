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
package com.facebook.presto.application;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Objects.requireNonNull;

public class StaticApplicationStore
{
    private static final Logger log = Logger.get(StaticApplicationStore.class);
    private static final File APPLICATION_CONFIGURATION_DIR = new File("etc/application/");
    private final ApplicationManager applicationManager;
    private final AtomicBoolean applicationsLoading = new AtomicBoolean();
    private final AtomicBoolean applicationsLoaded = new AtomicBoolean();

    @Inject
    public StaticApplicationStore(ApplicationManager applicationManager)
    {
        this.applicationManager = requireNonNull(applicationManager, "applicationManager is null");
    }

    public boolean areApplicationsLoaded()
    {
        return applicationsLoaded.get();
    }

    public void loadApplications()
            throws Exception
    {
        if (!applicationsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(APPLICATION_CONFIGURATION_DIR)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadApplication(file);
            }
        }

        applicationsLoaded.set(true);
    }

    private void loadApplication(File file)
            throws Exception
    {
        log.info("-- Loading application %s --", file);
        Map<String, String> properties = new HashMap<>(loadProperties(file));

        String applicationName = properties.remove("application.name");
        checkState(applicationName != null, "Application configuration %s does not contain application.name", file.getAbsoluteFile());

        applicationManager.setup(applicationName, ImmutableMap.copyOf(properties));
        log.info("-- Added application %s --", applicationName);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static Map<String, String> loadProperties(File file)
            throws Exception
    {
        requireNonNull(file, "file is null");

        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
