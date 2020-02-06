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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.getNameWithoutExtension;

public class StaticFunctionNamespaceStore
{
    private static final Logger log = Logger.get(StaticFunctionNamespaceStore.class);
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final FunctionManager functionManager;
    private final File configDir;
    private final AtomicBoolean functionNamespaceLoading = new AtomicBoolean();

    @Inject
    public StaticFunctionNamespaceStore(FunctionManager functionManager, StaticFunctionNamespaceStoreConfig config)
    {
        this.functionManager = functionManager;
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
                loadFunctionNamespaceManager(file);
            }
        }
    }

    private void loadFunctionNamespaceManager(File file)
            throws Exception
    {
        String catalogName = getNameWithoutExtension(file.getName());
        log.info("-- Loading function namespace manager from %s --", file);
        Map<String, String> properties = new HashMap<>(loadProperties(file));

        String functionNamespaceManagerName = properties.remove("function-namespace-manager.name");
        checkState(functionNamespaceManagerName != null, "Function namespace configuration %s does not contain function-namespace-manager.name", file.getAbsoluteFile());

        functionManager.loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
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
