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
package com.facebook.presto.dynamicCatalog;

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.spi.ConnectorId;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.google.common.base.Strings.nullToEmpty;

@Path("/v1/catalog")
public class DynamicCatalogController
{
    private final CatalogManager catalogManager;
    private final ConnectorManager connectorManager;
    private final Announcer announcer;
    private final InternalNodeManager internalNodeManager;
    private final File catalogConfigurationDir;
    private static final Logger log = Logger.get(DynamicCatalogController.class);
    private final ResponseParser responseParser;

    private DynamicCatalogController(CatalogManager catalogManager, ConnectorManager connectorManager, Announcer announcer, InternalNodeManager internalNodeManager, File catalogConfigurationDir, ResponseParser responseParser)
    {
        this.catalogManager = catalogManager;
        this.connectorManager = connectorManager;
        this.announcer = announcer;
        this.internalNodeManager = internalNodeManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.responseParser = responseParser;
    }

    @Inject
    public DynamicCatalogController(CatalogManager catalogManager, ConnectorManager connectorManager, Announcer announcer, InternalNodeManager internalNodeManager, StaticCatalogStoreConfig config, ResponseParser responseParser)
    {
        this(catalogManager, connectorManager, announcer, internalNodeManager, config.getCatalogConfigurationDir(), responseParser);
    }

    @Path("add")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addCatalog(CatalogVo catalogVo)
    {
        try {
            log.info("addCatalog : input values are " + catalogVo);
            ConnectorId connectorId = connectorManager.createConnection(catalogVo.getCatalogName(), catalogVo.getConnectorName(), catalogVo.getProperties());
            updateConnectorIdAnnouncement(announcer, connectorId, internalNodeManager);
            log.info("addCatalog() : catalogConfigurationDir: " + catalogConfigurationDir.getAbsolutePath());
            writeToFile(catalogVo);
            log.info("addCatalog() : Successfully added catalog " + catalogVo.getCatalogName());
            return successResponse(responseParser.build("Successfully added catalog: " + catalogVo.getCatalogName(), 200));
        }
        catch (Exception ex) {
            log.error("addCatalog() : Error adding catalog " + ex.getMessage());
            return failedResponse(responseParser.build("Error adding Catalog: " + ex.getMessage(), 500));
        }
    }

    private void writeToFile(CatalogVo catalogVo)
            throws Exception
    {
        final Properties properties = new Properties();
        properties.put("connector.name", catalogVo.getConnectorName());
        properties.putAll(catalogVo.getProperties());
        String filePath = getPropertyFilePath(catalogVo.getCatalogName());
        log.info("filepath: " + filePath);
        File propertiesFile = new File(filePath);
        try (OutputStream out = Files.newOutputStream(propertiesFile.toPath())) {
            properties.store(out, "adding catalog using endpoint");
        }
        catch (Exception ex) {
            log.error("error while writing to a file :" + ex.getMessage());
            throw new Exception("Error writing to file " + ex.getMessage());
        }
    }

    private String getPropertyFilePath(String catalogName)
    {
        return catalogConfigurationDir.getPath() + File.separator + catalogName + ".properties";
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, ConnectorId connectorId, InternalNodeManager nodeManager)
    {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
        nodeManager.refreshNodes();
    }

    private Response successResponse(ResponseParser responseParser)
    {
        return Response.status(Response.Status.OK).entity(responseParser).type(MediaType.APPLICATION_JSON).build();
    }

    private Response failedResponse(ResponseParser responseParser)
    {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseParser).type(MediaType.APPLICATION_JSON).build();
    }
}
