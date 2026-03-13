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
package com.facebook.presto.hive.security;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertThrows;

public class TestProcedureAccessControl
{
    @Test
    public void testDeniedWhenNoRulesConfigured()
    {
        ProcedureAccessControl procedureAccessControl = new ProcedureAccessControl(new SecurityConfig());
        assertDenied(() -> procedureAccessControl.checkCanCallProcedure(user("alice"), new SchemaTableName("system", "procedure1")));
    }

    @Test
    public void testAllowedAndDeniedWithRules()
            throws IOException
    {
        Path configFile = Files.createTempFile("hive-procedure-access-control", ".json");
        configFile.toFile().deleteOnExit();
        Files.writeString(configFile, "{\n" +
                "  \"procedures\": [\n" +
                "    {\n" +
                "      \"schema\": \"secret\",\n" +
                "      \"privileges\": []\n" +
                "    },\n" +
                "    {\n" +
                "      \"user\": \"admin\",\n" +
                "      \"schema\": \".*\",\n" +
                "      \"privileges\": [\"EXECUTE\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"user\": \"alice\",\n" +
                "      \"schema\": \"aliceschema\",\n" +
                "      \"privileges\": [\"EXECUTE\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"user\": \"bob\",\n" +
                "      \"schema\": \"bobschema\",\n" +
                "      \"procedure\": \"bobprocedure\",\n" +
                "      \"privileges\": [\"EXECUTE\"]\n" +
                "    }\n" +
                "  ]\n" +
                "}\n", UTF_8);

        ProcedureAccessControl procedureAccessControl = new ProcedureAccessControl(new SecurityConfig()
                .setProcedureAccessControlConfigFile(configFile.toString()));

        procedureAccessControl.checkCanCallProcedure(user("admin"), new SchemaTableName("bobschema", "bobprocedure"));
        procedureAccessControl.checkCanCallProcedure(user("alice"), new SchemaTableName("aliceschema", "aliceprocedure"));
        procedureAccessControl.checkCanCallProcedure(user("bob"), new SchemaTableName("bobschema", "bobprocedure"));

        assertDenied(() -> procedureAccessControl.checkCanCallProcedure(user("admin"), new SchemaTableName("secret", "secretprocedure")));
        assertDenied(() -> procedureAccessControl.checkCanCallProcedure(user("alice"), new SchemaTableName("bobschema", "bobprocedure")));
        assertDenied(() -> procedureAccessControl.checkCanCallProcedure(user("bob"), new SchemaTableName("aliceschema", "aliceprocedure")));
    }

    @Test
    public void testRefreshPeriodRequiresConfigFile()
    {
        SecurityConfig securityConfig = new SecurityConfig()
                .setProcedureAccessControlRefreshPeriod(new Duration(1, TimeUnit.SECONDS));

        assertThrows(IllegalArgumentException.class, () -> new ProcedureAccessControl(securityConfig));
    }

    private static ConnectorIdentity user(String name)
    {
        return new ConnectorIdentity(name, Optional.empty(), Optional.empty());
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
