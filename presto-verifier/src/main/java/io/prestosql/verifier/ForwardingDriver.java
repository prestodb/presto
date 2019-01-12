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
package io.prestosql.verifier;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

class ForwardingDriver
        implements Driver
{
    private final Driver driver;

    ForwardingDriver(Driver driver)
    {
        this.driver = requireNonNull(driver);
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return driver.acceptsURL(url);
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        return driver.connect(url, info);
    }

    @Override
    public int getMajorVersion()
    {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion()
    {
        return driver.getMinorVersion();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public boolean jdbcCompliant()
    {
        return driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        return driver.getParentLogger();
    }
}
