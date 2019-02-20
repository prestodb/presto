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
package com.facebook.presto.password.jdbc;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.mindrot.jbcrypt.BCrypt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Maps.fromProperties;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class UsersTool
{
    @Inject
    private Datastore datastore;
    private static final File CONFIG_FILE = new File("presto-main/etc/password-authenticator.properties");
    private static final String NAME_PROPERTY = "password-authenticator.name";

    public static void main(String[] args)
    {
        try {
            CommandLineParser parser = new DefaultParser();
            // create Options object
            Options options = new Options();

            options.addOption("u", true, "user name");
            options.addOption("p", true, "password");
            options.addOption("s", true, "schema");

            CommandLine cmd = parser.parse(options, args);
            String user = cmd.getOptionValue("u");
            String password = cmd.getOptionValue("p");
            String schema = cmd.getOptionValue("s");

            File configFileLocation = CONFIG_FILE.getAbsoluteFile();
            Map<String, String> config = new HashMap<>(loadProperties(configFileLocation));

            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(JdbcConfig.class);
                        binder.bind(Datastore.class).to(PostgresDatastore.class);
                        binder.bind(UsersTool.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setOptionalConfigurationProperties(config)
                    .initialize();

            UsersTool instance = injector.getInstance(UsersTool.class);
            instance.createUser(user, password, schema);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private boolean createUser(String user, String password, String schema)
            throws SQLException
    {
        String salt = BCrypt.gensalt();
        String hashedPassword = BCrypt.hashpw(password, salt);

        try (Connection connection = datastore.getConnection();
                PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO public.users" +
                        "(\"user\", \"password\", inserted_date, schema_name)" +
                        "VALUES(?, ?, NOW(), ?);")) {
            insertStatement.setString(1, user);
            insertStatement.setString(2, hashedPassword);
            insertStatement.setString(3, schema);
            return insertStatement.execute();
        }
    }

    private static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
