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

package com.facebook.presto.session;

import com.facebook.airlift.testing.TempFile;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.SessionConfigurationContext;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager.SystemSessionPropertyConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.session.FileSessionPropertyManager.CODEC;
import static org.testng.Assert.assertEquals;

public class TestFileSessionPropertyManager
{
    private static final SessionConfigurationContext CONTEXT = new SessionConfigurationContext(
            "user",
            Optional.of("source"),
            ImmutableSet.of("tag1", "tag2"),
            Optional.of(QueryType.DATA_DEFINITION.toString()),
            Optional.of(new ResourceGroupId(ImmutableList.of("global", "pipeline", "user_foo", "bar"))),
            Optional.of("bar"));

    @Test
    public void testResourceGroupMatch()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2");
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("global.pipeline.user_.*")),
                Optional.empty(),
                Optional.empty(),
                properties);

        assertProperties(properties, spec);
    }

    @Test
    public void testClientTagMatch()
            throws IOException
    {
        ImmutableMap<String, String> properties = ImmutableMap.of("PROPERTY", "VALUE");
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                properties);

        assertProperties(properties, spec);
    }

    @Test
    public void testMultipleMatch()
            throws IOException
    {
        SessionMatchSpec spec1 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1"));
        SessionMatchSpec spec2 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2"));

        assertProperties(ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2"), spec1, spec2);
    }

    @Test
    public void testNoMatch()
            throws IOException
    {
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("global.interactive.user_.*")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY", "VALUE"));

        assertProperties(ImmutableMap.of(), spec);
    }

    @Test
    public void testClientInfoMatch()
            throws IOException
    {
        ImmutableMap<String, String> properties = ImmutableMap.of("PROPERTY", "VALUE");
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("bar")),
                Optional.empty(),
                properties);

        assertProperties(properties, spec);
    }

    @Test
    public void testOverride()
             throws IOException
    {
        ImmutableMap<String, String> overrideProperties = ImmutableMap.of("PROPERTY1", "VALUE1");
        ImmutableMap<String, String> defaultProperties = ImmutableMap.of("PROPERTY1", "VALUE2", "PROPERTY2", "VALUE");

        SessionMatchSpec specOverride = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("bar")),
                Optional.of(true),
                overrideProperties);

        SessionMatchSpec specDefault = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("bar")),
                Optional.empty(),
                defaultProperties);

        // PROPERTY1 should be an override property with the value from the default (non-override, higher precendence)
        // spec.
        // PROPERTY2 should be a default property
        assertProperties(defaultProperties, ImmutableMap.of("PROPERTY1", "VALUE2"), specOverride, specDefault);
    }

    private static void assertProperties(Map<String, String> defaultProperties, SessionMatchSpec... spec)
            throws IOException
    {
        assertProperties(defaultProperties, ImmutableMap.of(), spec);
    }

    private static void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, SessionMatchSpec... spec)
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            Path configurationFile = tempFile.path();
            Files.write(configurationFile, CODEC.toJsonBytes(Arrays.asList(spec)));
            SessionPropertyConfigurationManager manager = new FileSessionPropertyManager(new FileSessionPropertyManagerConfig().setConfigFile(configurationFile.toFile()));
            SystemSessionPropertyConfiguration propertyConfiguration = manager.getSystemSessionProperties(CONTEXT);
            assertEquals(propertyConfiguration.systemPropertyDefaults, defaultProperties);
            assertEquals(propertyConfiguration.systemPropertyOverrides, overrideProperties);
        }
    }
}
