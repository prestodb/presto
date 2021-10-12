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
package com.facebook.presto.catalog;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.codehaus.plexus.util.StringUtils;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

@Path(value = "/api/v1/catalog")
public class DynamicCatalogStore {
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    public static final Map<String, CatalogInfo> CATALOG_INFO_MAP = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService;
    private final StaticCatalogStoreConfig config;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config) {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()), config);
    }

    public DynamicCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs, StaticCatalogStoreConfig config) {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
        this.config = config;
    }

    /**
     * 新增catalog
     *
     * @param catalogInfo catalog信息
     * @return response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(CatalogInfo catalogInfo) {
        try {
            writeFile(catalogInfo);
        } catch (IOException e) {
            log.error("write file error", e);
            return Response.status(500).entity("write file error").build();
        }
        loadCatalog(catalogInfo);
        return Response.ok().build();
    }

    /**
     * 删除catalog
     *
     * @param catalogInfo catalogInfo catalog信息
     * @return response
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response del(CatalogInfo catalogInfo) {
        delCatalogFile(catalogInfo);
        return Response.ok().build();
    }

    /**
     * 校验缓存中是否已经存在相同的Catalog
     *
     * @param catalogInfo catalog信息
     * @return true 存在，false 不存在
     */
    private boolean isDuplicate(CatalogInfo catalogInfo) {
        if (CATALOG_INFO_MAP.containsKey(catalogInfo.getCatalogName())) {
            return !MD5Util.getInstance().MD5(CATALOG_INFO_MAP.get(catalogInfo.getCatalogName()).toString()).equals(
                    MD5Util.getInstance().MD5(catalogInfo.toString()));
        }
        return false;
    }


    /**
     * 增加Catalog信息
     *
     * @param catalogInfo catalog信息
     */
    private void loadCatalog(CatalogInfo catalogInfo) {
        if (disabledCatalogs.contains(catalogInfo.getCatalogName())) {
            log.info("Skipping disabled catalog %s", catalogInfo.getCatalogName());
            return;
        }
        if (isDuplicate(catalogInfo)) {
            log.info("current catalog is having : {}", catalogInfo.getCatalogName());
            return;
        }
        log.info("-- Loading catalog %s --", catalogInfo.getCatalogName());
        checkState(catalogInfo.getConnectorName() != null, "Configuration for catalog %s does not contain connector.name", catalogInfo.getCatalogName());
        connectorManager.createConnection(catalogInfo.getCatalogName(), catalogInfo.getConnectorName(), catalogInfo.getProperties());
        log.info("-- Added catalog %s using connector %s --", catalogInfo.getCatalogName(), catalogInfo.getConnectorName());
    }

    /**
     * 删除Catalog文件
     *
     * @param catalogInfo catalog信息
     */
    private void delCatalogFile(CatalogInfo catalogInfo) {
        String filePath = "etc/catalog/" + catalogInfo.getCatalogName() + ".properties";
        FileUtil.delFile(filePath);
    }

    /**
     * 写配置文件信息
     *
     * @param catalogInfo catalog信息
     * @throws IOException IO异常
     */
    private void writeFile(CatalogInfo catalogInfo) throws IOException {
        writeSchemaToFile(catalogInfo);
        if (!isDuplicate(catalogInfo)) {
            writeCatalogToFile(catalogInfo);
        }
    }

    /**
     * 写catalog文件
     *
     * @param catalogInfo catalog信息
     * @throws IOException IO异常
     */
    private void writeCatalogToFile(CatalogInfo catalogInfo) throws IOException {
        File file = new File("etc/catalog/" + catalogInfo.getCatalogName() + ".properties");
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        FileUtil.writeProperties(file.getPath(), catalogInfo.getProperties(), false);
    }

    /**
     * 持久化Schema信息
     *
     * @param catalogInfo catalog 信息
     * @throws IOException IO异常
     */
    private void writeSchemaToFile(CatalogInfo catalogInfo) throws IOException {
        String connectorName = catalogInfo.getConnectorName();
        String tableName = catalogInfo.getTableName();
        Object schemaInfo = catalogInfo.getSchemaInfo();
        if (!(StringUtils.isEmpty(connectorName) || StringUtils.isEmpty(tableName) || Objects.isNull(schemaInfo))) {
            String filePath = "etc/" + connectorName + "/" + tableName + ".json";
            File file = new File(filePath);
            if (!file.getParentFile().exists()) {
                boolean finished = file.getParentFile().mkdirs();
                if (!finished) {
                    log.error(">>>>>>>>创建目录{}失败<<<<<<<<", file.getPath());
                    return;
                }
            }
            FileUtil.writeFile(filePath, JsonUtil.toJson(schemaInfo), false);
        }
    }

    public boolean areCatalogsLoaded() {
        return catalogsLoaded.get();
    }

    public void loadCatalogs() throws Exception {
        load();
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                reload();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 60, config.getIntervalTime(), TimeUnit.SECONDS);
    }

    private void reload() throws Exception {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                Map<String, String> properties = FileUtil.getAllProperties(file.getPath());
                if (isDuplicate(new CatalogInfo(Files.getNameWithoutExtension(file.getName()), properties.get("connector.name"), null, null, properties))) {
                    continue;
                }
                loadCatalog(file);
            }
        }
        catalogsLoaded.set(true);
    }

    public void load() throws Exception {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }
        catalogsLoaded.set(true);
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }
        additionalCatalogs.forEach(this::loadCatalog);
        catalogsLoaded.set(true);
    }

    private void loadCatalog(File file) throws Exception {
        String catalogName = Files.getNameWithoutExtension(file.getName());
        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());
        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties) {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }
        log.info("-- Loading catalog %s --", catalogName);
        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            } else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }
        CATALOG_INFO_MAP.put(catalogName, new CatalogInfo(catalogName, connectorName, null, null, properties));
        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);
        connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
