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
package com.facebook.presto.verifier.framework;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class JdbcDriverUtil
{
    private JdbcDriverUtil()
    {
    }

    public static void initializeDrivers(
            Optional<String> additionalJdbcDriverPath,
            Optional<String> controlJdbcDriverClass,
            Optional<String> testJdbcDriverClass)
    {
        if (additionalJdbcDriverPath.isPresent()) {
            List<URL> urlList = getUrls(additionalJdbcDriverPath.get());
            URL[] urls = new URL[urlList.size()];
            urlList.toArray(urls);
            controlJdbcDriverClass.ifPresent(driver -> loadJdbcDriver(urls, driver));
            testJdbcDriverClass.ifPresent(driver -> loadJdbcDriver(urls, driver));
        }
    }

    private static void loadJdbcDriver(URL[] urls, String jdbcClassName)
    {
        try (URLClassLoader classLoader = new URLClassLoader(urls)) {
            Driver driver = (Driver) Class.forName(jdbcClassName, true, classLoader).getConstructor().newInstance();
            // The code calling the DriverManager to load the driver needs to be in the same class loader as the driver
            // In order to bypass this we create a shim that wraps the specified jdbc driver class.
            // TODO: Change the implementation to be DataSource based instead of DriverManager based.
            DriverManager.registerDriver(new ForwardingDriver(driver));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (SQLException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<URL> getUrls(String path)
    {
        try {
            ImmutableList.Builder<URL> urlList = ImmutableList.builder();
            File driverPath = new File(path);
            if (!driverPath.isDirectory()) {
                return urlList.add(Paths.get(path).toUri().toURL()).build();
            }
            File[] files = driverPath.listFiles((dir, name) -> name.endsWith(".jar"));
            if (files == null) {
                return urlList.build();
            }
            for (File file : files) {
                // Does not handle nested directories
                if (file.isDirectory()) {
                    continue;
                }
                urlList.add(Paths.get(file.getAbsolutePath()).toUri().toURL());
            }
            return urlList.build();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
