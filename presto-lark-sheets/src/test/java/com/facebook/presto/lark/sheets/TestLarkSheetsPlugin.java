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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;

import static com.facebook.presto.lark.sheets.LarkSheetsConfig.Domain.FEISHU;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertNotNull;

public class TestLarkSheetsPlugin
{
    private static final String SECRET_FILE = "testing-app-secret-base64";

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new LarkSheetsPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(getTestingConnectorConfig())
                .build();
        Connector connector = factory.create("lark_sheets", properties, new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }

    public static Map<String, String> getTestingConnectorConfig()
    {
        return ImmutableMap.of(
                "app-domain", FEISHU.name(),
                "app-id", "cli_a16fc7ddf4b9900d",
                "app-secret-file", getTestingAppSecretFile());
    }

    private static String getTestingAppSecretFile()
    {
        URL url = Resources.getResource(SECRET_FILE);
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(url.getPath()));
            byte[] decoded = Base64.getDecoder().decode(bytes);
            Path targetFile = Files.createTempFile("app-secret-", ".json");
            Files.write(targetFile, decoded);
            return targetFile.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not get secret file", e);
        }
    }
}
