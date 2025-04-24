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
            assertEquals("User credentials in both configs and url", e.getMessage());
        }
    }

    @Test
    public void testConnectionFactory_credentialsInUrlNotInConfig() throws SQLException
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl("jdbc:oracle:thin:user/pass@localhost:1521/database");
        config.setConnectionUser(null);
        config.setConnectionPassword(null);

        OracleConfig oracleConfig = new OracleConfig();
        oracleConfig.setSynonymsEnabled(true);

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
        }
        catch (PrestoException e) {
            fail("Expected no exception but got this" + e.getMessage());
        }
    }

    @Test
    public void testConnectionFactory_credentialsInConfigNotInUrl() throws SQLException
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl("jdbc:oracle:thin:@localhost:1521/database");
        config.setConnectionUser("user");
        config.setConnectionPassword("pass");

        OracleConfig oracleConfig = new OracleConfig();
        oracleConfig.setSynonymsEnabled(true);

        try {
            OracleClientModule.connectionFactory(config, oracleConfig);
        }
        catch (PrestoException e) {
            fail("Expected no exception but got this: " + e.getMessage());
        }
    }
}
