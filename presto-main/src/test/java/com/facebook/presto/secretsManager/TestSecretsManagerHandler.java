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
package com.facebook.presto.secretsManager;

import com.facebook.presto.spi.secretsManager.SecretsManager;
import com.facebook.presto.spi.secretsManager.SecretsManagerFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestSecretsManagerHandler
{
    public static final String INVALID = "invalid";
    private SecretsManagerHandler testSecretsManagerHandler;

    @BeforeClass
    public void setup() throws IOException
    {
        testSecretsManagerHandler = new SecretsManagerHandler();
        SecretsManagerFactory factory = new TestingSecretsManagerFactory();
        testSecretsManagerHandler.addSecretsManagerFactory(factory);
        testSecretsManagerHandler.loadSecretsManagerConfiguration(ImmutableMap.of("secrets-manager.name", "test-secrets-manager"));
    }

    @Test
    public void testLoadSecrets() throws IOException
    {
        Map<String, Map<String, String>> secrets = testLoadSecrets(testSecretsManagerHandler.getSecretsManagerName());
        assertEquals(secrets.size(), 1);
    }
    @Test
    public void testInvalidSecretsManager() throws IOException
    {
        assertThrows(RuntimeException.class, () -> testLoadSecrets(INVALID));
    }

    @Test
    public void testInvalidConfig() throws IOException
    {
        SecretsManagerFactory factory = new InvalidSecretsManagerFactory();
        testSecretsManagerHandler.addSecretsManagerFactory(factory);
        assertThrows(RuntimeException.class, () -> testLoadSecrets(INVALID));
    }

    private Map<String, Map<String, String>> testLoadSecrets(String secretsManager) throws IOException
    {
        return testSecretsManagerHandler.loadSecrets(secretsManager);
    }

    private class InvalidSecretsManagerFactory
            implements SecretsManagerFactory
    {
        @Override
        public String getName()
        {
            return INVALID;
        }

        @Override
        public SecretsManager create()
        {
            return null;
        }
    }
}
