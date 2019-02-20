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

import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class JdbcPasswordAuthenticationFactoryTest
{
    @Test
    public void createPlugin()
    {
        JdbcPasswordAuthenticationFactory jdbcPasswordAuthenticationFactory = new JdbcPasswordAuthenticationFactory();

        String connectionURL = "jdbc:postgresql://localhost:5432/DYNAMIC_SCHEMA_METASTORE";
        String user = "postgres";
        String pass = "Datorama01";

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("jdbc.auth.type", "postgresql")
                .put("jdbc.auth.url", connectionURL)
                .put("jdbc.auth.schema", "public")
                .put("jdbc.auth.table", "users")
                .put("jdbc.auth.user", user)
                .put("jdbc.auth.password", pass)
                .put("jdbc.auth.cache-ttl", "1s")
                .build();

        String authUser = "admin";
        String authPassword = "admin";
        PasswordAuthenticator passwordAuthenticator = jdbcPasswordAuthenticationFactory.create(properties);
        Principal authenticatedPrincipal = passwordAuthenticator.createAuthenticatedPrincipal(authUser, authPassword);
        assertEquals(authenticatedPrincipal.getName(), authUser);
    }
}
