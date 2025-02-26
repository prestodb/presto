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
package com.facebook.presto.server;

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorTypeSerdeManager;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.facebook.presto.server.CatalogResource.CatalogOperation.ADD;
import static com.facebook.presto.server.CatalogResource.CatalogOperation.REMOVE;
import static com.facebook.presto.server.CatalogResource.CatalogOperation.UPDATE;
import static com.facebook.presto.server.PrestoServer.getPrestoAnnouncement;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;

@Path("/v1/catalog")
@RolesAllowed({USER, ADMIN})
public class CatalogResource
{
    private static final Logger log = Logger.get(CatalogResource.class);
    private final StaticCatalogStore catalogStore;
    private final ConnectorTypeSerdeManager connectorTypeSerdeManager;
    private final Announcer announcer;
    private final CatalogManager catalogManager;
    private final InternalNodeManager nodeManager;
    private final FeaturesConfig featuresConfig;
    private final Lock announcerLock = new ReentrantLock();
    private final QueryManager queryManager;
    private final Predicate<BasicQueryInfo> nonNull = Objects::nonNull;
    private final Predicate<BasicQueryInfo> isRunning = this::isRunning;
    private static final Lock catalogLock = new ReentrantLock();

    @Inject
    public CatalogResource(StaticCatalogStore catalogStore,
                           Announcer announcer,
                           CatalogManager catalogManager,
                           ConnectorTypeSerdeManager connectorTypeSerdeManager,
                           InternalNodeManager nodeManager,
                           QueryManager queryManager,
                           FeaturesConfig featuresConfig)
    {
        this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.connectorTypeSerdeManager = requireNonNull(connectorTypeSerdeManager, "connectorMetadataUpdateHandleSerdeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public Response getCatalogList()
    {
        final ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        final String property = announcement.getProperties().get("connectorIds");
        List<String> catalogList = Collections.emptyList();
        if (!isNullOrEmpty(property)) {
            catalogList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(property);
        }
        return Response.ok(catalogList).build();
    }

    @GET
    @Path("/{catalogName:[0-9a-z_]*}/status")
    @Produces(APPLICATION_JSON)
    public Response getCatalogStatus(@PathParam("catalogName") String catalogName)
    {
        Response.Status status = NOT_FOUND;
        final Map<String, String> apiResponse = new HashMap<>();
        ConnectorId connectorId = getCatalogConnectorId(catalogName);
        if (Objects.nonNull(connectorId)) {
            final boolean catalogInUse = isCatalogInUse(connectorId);
            if (catalogInUse) {
                apiResponse.put("status", "catalog_in_use");
                apiResponse.put("message", format("The given catalog %s is currently in use", catalogName));
            }
            else {
                apiResponse.put("status", "catalog_not_in_use");
                apiResponse.put("message", format("The given catalog %s is not in use", catalogName));
            }
            status = OK;
        }
        else {
            apiResponse.put("status", "catalog_not_found");
            apiResponse.put("message", format("The given catalog %s was not found", catalogName));
            log.info("Catalog ['%s'] Not found", catalogName);
        }
        return Response.status(status).entity(apiResponse).build();
    }

    @POST
    @Path("/{catalogName:[0-9a-z_]*}")
    @Produces(APPLICATION_JSON)
    @Consumes(APPLICATION_JSON)
    public Response addCatalog(@Context HttpServletRequest servletRequest,
                               @PathParam("catalogName") String catalogName,
                               Map<String, String> properties) throws WebApplicationException
    {
        log.info("addCatalog::%s", catalogName);
        if (featuresConfig.isRestrictCatalogEndpointsLocally() && !isLocalhost(servletRequest)) {
            log.info("Blocking non Localhost addCatalog %s", catalogName);
            return Response.status(FORBIDDEN).build();
        }
        return executeCatalogOperation(ADD, catalogName, properties, servletRequest);
    }

    private ConnectorId registerCatalog(String catalogName, Map<String, String> properties)
    {
        return catalogStore.loadCatalog(catalogName, properties);
    }

    @DELETE
    @Path("/{catalogName:[0-9a-z_]*}")
    @Produces(APPLICATION_JSON)
    @Consumes(APPLICATION_JSON)
    public Response removeCatalog(@Context HttpServletRequest servletRequest,
                                  @PathParam("catalogName") String catalogName) throws WebApplicationException
    {
        log.info("removeCatalog::%s", catalogName);
        if (featuresConfig.isRestrictCatalogEndpointsLocally() && !isLocalhost(servletRequest)) {
            log.info("Blocking non Localhost removeCatalog %s", catalogName);
            return Response.status(FORBIDDEN).build();
        }
        return executeCatalogOperation(REMOVE, catalogName, ImmutableMap.of(), servletRequest);
    }

    @PUT
    @Path("/{catalogName:[0-9a-z_]*}")
    @Produces(APPLICATION_JSON)
    @Consumes(APPLICATION_JSON)
    public Response updateCatalog(@Context HttpServletRequest servletRequest,
                                  @PathParam("catalogName") String catalogName,
                                  Map<String, String> properties) throws WebApplicationException
    {
        log.info("updateCatalog::%s", catalogName);
        if (featuresConfig.isRestrictCatalogEndpointsLocally() && !isLocalhost(servletRequest)) {
            log.info("Blocking non Localhost updateCatalog %s", catalogName);
            return Response.status(FORBIDDEN).build();
        }
        return executeCatalogOperation(UPDATE, catalogName, properties, servletRequest);
    }

    private Response executeCatalogOperation(CatalogOperation operation, String catalogName,
                                             Map<String, String> properties, HttpServletRequest servletRequest)
    {
        catalogLock.lock();
        try {
            final Principal principal = servletRequest.getUserPrincipal();
            Response.Status status;
            ConnectorId connectorId;
            switch (operation) {
                case ADD:
                    status = CONFLICT;
                    if (!isCatalogPresent(catalogName)) {
                        connectorId = registerCatalog(catalogName, properties);
                        if (Objects.nonNull(connectorId)) {
                            updateConnectorIds(connectorId, false);
                            log.info("Catalog ['%s'] is added to Presto by user ['%s']", catalogName, principal);
                            status = CREATED;
                        }
                    }
                    else {
                        log.info("Catalog ['%s'] is already present in Presto", catalogName);
                    }
                    break;
                case REMOVE:
                    connectorId = getCatalogConnectorId(catalogName);
                    if (Objects.nonNull(connectorId)) {
                        catalogStore.dropConnection(catalogName);
                        connectorTypeSerdeManager.removeConnectorTypeSerdeProvider(connectorId);
                        updateConnectorIds(connectorId, true);
                        log.info("Catalog ['%s'] is removed by user ['%s']", catalogName, principal);
                        status = NO_CONTENT;
                    }
                    else {
                        log.info("Catalog ['%s'] Not found", catalogName);
                        status = NOT_FOUND;
                    }
                    break;
                case UPDATE:
                    connectorId = getCatalogConnectorId(catalogName);
                    if (Objects.nonNull(connectorId)) {
                        catalogStore.dropConnection(catalogName);
                        connectorTypeSerdeManager.removeConnectorTypeSerdeProvider(connectorId);
                        updateConnectorIds(connectorId, true);
                        log.info("Catalog ['%s'] is removed by user ['%s']", catalogName, principal);
                        status = NO_CONTENT;
                    }
                    else {
                        log.info("Catalog ['%s'] Not found", catalogName);
                        status = CREATED;
                    }
                    connectorId = registerCatalog(catalogName, properties);
                    if (Objects.nonNull(connectorId)) {
                        updateConnectorIds(connectorId, false);
                        log.info("Catalog ['%s'] is updated by user ['%s']", catalogName, principal);
                    }
                    else {
                        log.info("Unable to create Connection for catalog ['%s']", catalogName);
                        status = CONFLICT;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported catalog operation: " + operation);
            }
            return Response.status(status).build();
        }
        finally {
            catalogLock.unlock();
        }
    }

    private boolean isCatalogPresent(String catalogName)
    {
        return catalogManager.getCatalog(catalogName).isPresent();
    }

    private ConnectorId getCatalogConnectorId(String catalogName)
    {
        final Optional<Catalog> catalog = catalogManager.getCatalog(catalogName);
        return catalog.map(Catalog::getConnectorId).orElse(null);
    }

    private void updateConnectorIds(ConnectorId connectorId, boolean remove)
    {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        log.debug("announcement::%s", announcement);
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        log.info("Announcement properties::%s", properties);
        String property = nullToEmpty(properties.get("connectorIds"));
        log.info("connectorIds::%s", property);
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);
        if (remove) {
            //remove existing ConnectorId
            connectorIds.remove(connectorId.toString());
        }
        else {
            connectorIds.add(connectorId.toString());
        }
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));
        log.info("Updated connectorIds::%s", properties.get("connectorIds"));
        announcerLock.lock();
        try {
            // update announcement
            announcer.removeServiceAnnouncement(announcement.getId());
            announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
            announcer.forceAnnounce();
            nodeManager.refreshNodes();
        }
        finally {
            announcerLock.unlock();
        }
    }

    private boolean isLocalhost(final HttpServletRequest servletRequest)
    {
        try {
            final InetAddress localAddress = InetAddress.getByName(servletRequest.getLocalAddr());
            final InetAddress remoteAddress = InetAddress.getByName(servletRequest.getRemoteAddr());
            log.debug("localAddress::['%s'],remoteAddress::['%s']", localAddress, remoteAddress);
            return localAddress.isLoopbackAddress() || localAddress.equals(remoteAddress);
        }
        catch (UnknownHostException exc) {
            // Handle exception, possibly log it
            return false;
        }
    }
    private boolean isCatalogInUse(ConnectorId connectorId)
    {
        boolean catalogInUse = queryManager.getQueries().stream()
                .filter(nonNull.and(isRunning))
                .map(BasicQueryInfo::getQueryId)
                .map(queryId -> {
                    try {
                        return queryManager.getFullQueryInfo(queryId);
                    }
                    catch (NoSuchElementException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .anyMatch(queryInfo ->
                {
                    for (Input input : queryInfo.getInputs()) {
                        if (input.getConnectorId() == connectorId) {
                            return true;
                        }
                    }
                    return queryInfo.getOutput().isPresent() &&
                            queryInfo.getOutput().get().getConnectorId().equals(connectorId);
                });
        log.info("ConnectorId::['%s'] isCatalogInUse [%s]", connectorId, catalogInUse);
        return catalogInUse;
    }

    private Set<ConnectorId> extractConnectorIds(QueryInfo queryInfo)
    {
        ImmutableSet.Builder<ConnectorId> connectors = ImmutableSet.builder();
        for (Input input : queryInfo.getInputs()) {
            connectors.add(input.getConnectorId());
        }
        if (queryInfo.getOutput().isPresent()) {
            connectors.add(queryInfo.getOutput().get().getConnectorId());
        }
        return connectors.build();
    }

    private boolean isRunning(BasicQueryInfo queryInfo)
    {
        return !queryInfo.getState().isDone();
    }

    enum CatalogOperation
    {
        ADD, REMOVE, UPDATE
    }
}
