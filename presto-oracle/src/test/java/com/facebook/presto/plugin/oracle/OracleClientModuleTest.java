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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.testing.assertions.Assert.fail;

public class OracleClientModuleTest
{
    @Test
    public void testConnectionFactory_credentialsInUrlAndConfig() throws SQLException
    {
        BaseJdbcConfig config = new BaseJdbcConfig();

        // Credentials in both config and URL
        config.setConnectionUrl("jdbc:oracle:thin:user/pass@localhost:1521/database");
        config.setConnectionUser("user");
        config.setConnectionPassword("pass");

        OracleConfig oracleConfig = new OracleConfig();
        oracleConfig.setSynonymsEnabled(true);

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
            fail("Expected SQLException to be thrown");
        }
        catch (PrestoException e) {
            assertEquals("Credentials are specified in both connection-url and user/password properties. Use only one", e.getMessage());
        }

        // User in URL and password in config
        config.setConnectionUrl("jdbc:oracle:thin:user/@localhost:1521/database");
        config.setConnectionUser(null);
        config.setConnectionPassword("pass");

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
            fail("Expected SQLException to be thrown");
        }
        catch (PrestoException e) {
            assertEquals("Credentials are specified in both connection-url and user/password properties. Use only one", e.getMessage());
        }
        // User in config and password in URL
        config.setConnectionUrl("jdbc:oracle:thin:/pass@localhost:1521/database");
        config.setConnectionUser("user");
        config.setConnectionPassword(null);

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
            fail("Expected SQLException to be thrown");
        }
        catch (PrestoException e) {
            assertEquals("Credentials are specified in both connection-url and user/password properties. Use only one", e.getMessage());
        }

        // Credentials in URL, not in config
        config.setConnectionUrl("jdbc:oracle:thin:user/pass@localhost:1521/database");
        config.setConnectionUser(null);
        config.setConnectionPassword(null);

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
        }
        catch (PrestoException e) {
            fail("Expected no exception but got this" + e.getMessage());
        }

        // Credentials in config, not in URL
        config.setConnectionUrl("jdbc:oracle:thin:@localhost:1521/database");
        config.setConnectionUser("user");
        config.setConnectionPassword("pass");

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
        }
        catch (PrestoException e) {
            fail("Expected no exception but got this: " + e.getMessage());
        }

        // No credentials in either config or URL
        config.setConnectionUrl("jdbc:oracle:thin:@localhost:1521/database");
        config.setConnectionUser(null);
        config.setConnectionPassword(null);

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
        }
        catch (PrestoException e) {
            fail("Expected no exception, but got: " + e.getMessage(), e);
        }
    }
}
