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
package com.facebook.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.Files.getNameWithoutExtension;

public class StaticFunctionNamespaceStore
{
    private static final Logger log = Logger.get(StaticFunctionNamespaceStore.class);
    private static final String FUNCTION_NAMESPACE_MANAGER_NAME = "function-namespace-manager.name";

    private final FunctionAndTypeManager functionAndTypeManager;
    private final File configDir;
    private final AtomicBoolean functionNamespaceLoading = new AtomicBoolean();

    @Inject
    public StaticFunctionNamespaceStore(FunctionAndTypeManager functionAndTypeManager, StaticFunctionNamespaceStoreConfig config)
    {
        this.functionAndTypeManager = functionAndTypeManager;
        this.configDir = config.getFunctionNamespaceConfigurationDir();
    }

    public void loadFunctionNamespaceManagers()
            throws Exception
    {
        if (!functionNamespaceLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(configDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                String catalogName = getNameWithoutExtension(file.getName());
                Map<String, String> properties = loadProperties(file);
                checkState(!isNullOrEmpty(properties.get(FUNCTION_NAMESPACE_MANAGER_NAME)),
                        "Function namespace configuration %s does not contain %s",
                        file.getAbsoluteFile(),
                        FUNCTION_NAMESPACE_MANAGER_NAME);
                loadFunctionNamespaceManager(catalogName, properties);
            }
        }
    }

    public void loadFunctionNamespaceManagers(Map<String, Map<String, String>> catalogProperties)
    {
        catalogProperties.entrySet().stream()
                .forEach(entry -> loadFunctionNamespaceManager(entry.getKey(), entry.getValue()));
    }

    private void loadFunctionNamespaceManager(String catalogName, Map<String, String> properties)
    {
        log.info("-- Loading function namespace manager for catalog %s --", catalogName);
        properties = new HashMap<>(properties);
        String functionNamespaceManagerName = properties.remove(FUNCTION_NAMESPACE_MANAGER_NAME);
        checkState(!isNullOrEmpty(functionNamespaceManagerName), "%s property must be present", FUNCTION_NAMESPACE_MANAGER_NAME);

        functionAndTypeManager.loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
        log.info("-- Added function namespace manager [%s] --", catalogName);
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
