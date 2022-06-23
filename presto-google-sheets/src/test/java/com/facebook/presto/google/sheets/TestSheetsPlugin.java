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
package com.facebook.presto.google.sheets;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import static com.facebook.presto.google.sheets.TestGoogleSheets.GOOGLE_SHEETS;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertNotNull;

public class TestSheetsPlugin
{
    static final String TEST_METADATA_SHEET_ID = "1fu6qjHrOARa7Rqezfgd9C--XPCUOkN_vhvwpxZ4INGE#Tables";

    static String getTestCredentialsPath()
    {
        return Resources.getResource("gsheet-test-account.json").getPath();
    }

    @Test
    public void testCreateConnector()
            throws Exception
    {
        Plugin plugin = new SheetsPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        ImmutableMap.Builder<String, String> propertiesMap = new ImmutableMap.Builder<String, String>().put("credentials-path", getTestCredentialsPath()).put("metadata-sheet-id", TEST_METADATA_SHEET_ID);
        Connector connector = factory.create(GOOGLE_SHEETS, propertiesMap.build(), new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }
}
