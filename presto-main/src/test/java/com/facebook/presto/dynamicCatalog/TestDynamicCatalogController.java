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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StatusResponseHandler;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.testing.Closeables;
import com.facebook.presto.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDynamicCatalogController
{
    private static final JsonCodec<CatalogVo> catalogVoJsonCodec = JsonCodec.jsonCodec(CatalogVo.class);
    private TestingPrestoServer server;
    private HttpClient client;
    private static final Logger log = Logger.get(TestDynamicCatalogController.class);

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
        cleanUpCatalog();
    }

    @SuppressWarnings("deprecation")
    @AfterMethod
    public void teardown()
    {
        cleanUpCatalog();
        Closeables.closeQuietly(server);
        Closeables.closeQuietly(client);
    }

    @Test
    public void testAddCatalog()
    {
        CatalogVo catalogVo = getFakeCatalogObject("system", "testing");
        assertEquals(executeAddCatalogCall(catalogVo).getStatusCode(), 200);
    }

    @Test
    public void testAddCatalogFailed()
    {
        CatalogVo catalogVo = getFakeCatalogObject("invalidConnector", "invalidCatalog");
        assertEquals(executeAddCatalogCall(catalogVo).getStatusCode(), 500);
    }

    private URI uriFor(String path)
    {
        return uriBuilderFrom(server.getBaseUrl()).replacePath(path).build();
    }

    private CatalogVo getFakeCatalogObject(String connectorName, String catalogName)
    {
        CatalogVo catalogVo = new CatalogVo();
        catalogVo.setCatalogName(catalogName);
        catalogVo.setConnectorName(connectorName);
        Map<String, String> map = new HashMap<>();
        map.put("connector.name", connectorName);
        map.put("connection-url", "jdbc:postgresql://localhost:5432/postgres");
        map.put("connection-user", "postgres");
        map.put("connection-password", "postgres");
        catalogVo.setProperties(map);
        return catalogVo;
    }

    private void cleanUpCatalog()
    {
        if (deleteCatalog()) {
            log.debug("TestDynamicCatalogController:cleanUpCatalog() Successfully deleted catalog");
        }
        else {
            log.debug("TestDynamicCatalogController:cleanUpCatalog() Not able to deleted catalog");
        }
    }

    private boolean deleteCatalog()
    {
        String catalogName = getFakeCatalogObject("system", "testing").getCatalogName();
        return deletePropertyFile(catalogName);
    }

    private boolean deletePropertyFile(String catalogName)
    {
        return new File(getPropertyFilePath(catalogName)).delete();
    }

    private String getPropertyFilePath(String catalogName)
    {
        File catalogConfigurationDir = new File("etc/catalog/");
        return catalogConfigurationDir.getPath() + File.separator + catalogName + ".properties";
    }

    private StatusResponseHandler.StatusResponse executeAddCatalogCall(CatalogVo catalogVo)
    {
        Request request = preparePost().setHeader(PRESTO_USER, "user")
                .setUri(uriFor("/v1/catalog/add"))
                .setBodyGenerator(jsonBodyGenerator(catalogVoJsonCodec, catalogVo))
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_TIME_ZONE, "invalid time zone")
                .setHeader("Content-Type", "application/json")
                .build();
        StatusResponseHandler.StatusResponse response = client.execute(
                request,
                createStatusResponseHandler());
        return response;
    }
}
