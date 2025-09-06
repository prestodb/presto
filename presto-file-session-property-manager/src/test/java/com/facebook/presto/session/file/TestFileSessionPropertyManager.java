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

package com.facebook.presto.session.file;

import com.facebook.airlift.testing.TempFile;
import com.facebook.presto.session.AbstractTestSessionPropertyManager;
import com.facebook.presto.session.SessionMatchSpec;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.session.file.FileSessionPropertyManager.CODEC;
import static org.testng.Assert.assertEquals;

public class TestFileSessionPropertyManager
        extends AbstractTestSessionPropertyManager
{
    @Override
    protected void assertProperties(Map<String, String> defaultProperties, SessionMatchSpec... specs)
            throws IOException
    {
        assertProperties(defaultProperties, ImmutableMap.of(), ImmutableMap.of(), specs);
    }

    @Override
    protected void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, SessionMatchSpec... specs)
            throws IOException
    {
        assertProperties(defaultProperties, overrideProperties, ImmutableMap.of(), specs);
    }

    protected void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, Map<String, Map<String, String>> catalogProperties, SessionMatchSpec... specs)
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            Path configurationFile = tempFile.path();
            Files.write(configurationFile, CODEC.toJsonBytes(Arrays.asList(specs)));
            SessionPropertyConfigurationManager manager = new FileSessionPropertyManager(new FileSessionPropertyManagerConfig().setConfigFile(configurationFile.toFile()));
            SessionPropertyConfigurationManager.SystemSessionPropertyConfiguration propertyConfiguration = manager.getSystemSessionProperties(CONTEXT);
            assertEquals(propertyConfiguration.systemPropertyDefaults, defaultProperties);
            assertEquals(propertyConfiguration.systemPropertyOverrides, overrideProperties);
            assertEquals(manager.getCatalogSessionProperties(CONTEXT), catalogProperties);
        }
    }

    @Test
    public void testNullSessionProperties()
            throws IOException
    {
        ImmutableMap<String, Map<String, String>> catalogProperties = ImmutableMap.of("CATALOG", ImmutableMap.of("PROPERTY", "VALUE"));
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                null,
                catalogProperties);

        assertProperties(ImmutableMap.of(), ImmutableMap.of(), catalogProperties, spec);
    }

    @Test
    public void testNullCatalogSessionProperties()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2");
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                properties,
                null);

        assertProperties(properties, spec);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Either sessionProperties or catalogSessionProperties must be provided")
    public void testNullBothSessionProperties()
            throws IOException
    {
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                null,
                null);

        assertProperties(ImmutableMap.of(), spec);
    }
}
