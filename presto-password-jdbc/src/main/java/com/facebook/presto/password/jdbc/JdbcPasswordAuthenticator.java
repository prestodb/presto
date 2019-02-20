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

import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import org.mindrot.jbcrypt.BCrypt;

import javax.inject.Inject;

import java.security.Principal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JdbcPasswordAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(JdbcPasswordAuthenticator.class);

    @Inject
    private Datastore datastore;

    @Inject
    private JdbcConfig jdbcConfig;

    private final LoadingCache<Credentials, Principal> authenticationCache;
    private final String selectUserSQL = "select \"user\", \"password\" from %s.%s where \"user\"=?";

    @Inject
    public JdbcPasswordAuthenticator(JdbcConfig jdbcConfig, Datastore datastore)
    {
        this.datastore = datastore;
        this.jdbcConfig = jdbcConfig;
        this.authenticationCache = CacheBuilder.newBuilder()
                .expireAfterWrite(jdbcConfig.getJdbcAuthCacheTtl().toMillis(), MILLISECONDS)
                .build(CacheLoader.from(this::authenticate));
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        try {
            return authenticationCache.getUnchecked(new Credentials(user, password));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), AccessDeniedException.class);
            throw e;
        }
    }

    private Principal authenticate(Credentials credentials)
    {
        return authenticate(credentials.getUser(), credentials.getPassword());
    }

    private Principal authenticate(String user, String password)
    {
        String selectFormattedUserSQL = String.format(selectUserSQL, jdbcConfig.getJdbcAuthSchema(), jdbcConfig.getJdbcAuthTable());
        try (Connection connection = datastore.getConnection();
                PreparedStatement statement = connection.prepareStatement(selectFormattedUserSQL)) {
            statement.setString(1, user);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    if (verifyHash(password, resultSet.getString("password"))) {
                        log.debug("Authentication successful for user [%s]", user);
                        return new BasicPrincipal(user);
                    }
                    else {
                        log.debug("Authentication failed for user [%s]", user);
                        throw new AccessDeniedException("Invalid credentials");
                    }
                }
            }
        }
        catch (SQLException e) {
            log.debug(e, "Authentication error for user [%s]", user);
            throw new RuntimeException("Authentication error");
        }
        log.debug("Authentication error for user [%s]", user);
        throw new RuntimeException("Authentication error");
    }

    public String hash(String password, String salt)
    {
        return BCrypt.hashpw(password, salt);
    }

    public boolean verifyHash(String password, String hash)
    {
        return BCrypt.checkpw(password, hash);
    }

    private static class Credentials
    {
        private final String user;
        private final String password;

        private Credentials(String user, String password)
        {
            this.user = requireNonNull(user);
            this.password = requireNonNull(password);
        }

        public String getUser()
        {
            return user;
        }

        public String getPassword()
        {
            return password;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Credentials that = (Credentials) o;

            return Objects.equals(this.user, that.user) &&
                    Objects.equals(this.password, that.password);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, password);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("user", user)
                    .add("password", password)
                    .toString();
        }
    }
}
