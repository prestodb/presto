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

import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.SessionConfigurationContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Test(singleThreaded = true)
public abstract class AbstractTestSessionPropertyManager
{
    protected static final SessionConfigurationContext CONTEXT = new SessionConfigurationContext(
            "user",
            Optional.empty(),
            Optional.of("source"),
            ImmutableSet.of("tag1", "tag2"),
            Optional.of(QueryType.DATA_DEFINITION.toString()),
            Optional.of(new ResourceGroupId(ImmutableList.of("global", "pipeline", "user_foo", "bar"))),
            Optional.of("bar"),
            "testversion");

    protected abstract void assertProperties(Map<String, String> properties, SessionMatchSpec... spec)
            throws IOException;
    protected abstract void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, SessionMatchSpec... spec)
            throws IOException;
    protected abstract void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, Map<String, Map<String, String>> catalogProperties, SessionMatchSpec... spec)
            throws IOException;

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
                properties,
                ImmutableMap.of());

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
                properties,
                ImmutableMap.of());

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
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY3", "VALUE3"),
                ImmutableMap.of());
        SessionMatchSpec spec2 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2"),
                ImmutableMap.of());

        assertProperties(ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2", "PROPERTY3", "VALUE3"), spec1, spec2);
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
                ImmutableMap.of("PROPERTY", "VALUE"),
                ImmutableMap.of());

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
                properties,
                ImmutableMap.of());

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
                overrideProperties,
                ImmutableMap.of());

        SessionMatchSpec specDefault = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("bar")),
                Optional.empty(),
                defaultProperties,
                ImmutableMap.of());

        // PROPERTY1 should be an override property with the value from the default (non-override, higher precedence)
        // spec.
        // PROPERTY2 should be a default property
        assertProperties(defaultProperties, ImmutableMap.of("PROPERTY1", "VALUE2"), specOverride, specDefault);
    }

    @Test
    public void testCatalogProperty()
            throws IOException
    {
        ImmutableMap<String, String> defaultProperties = ImmutableMap.of("PROPERTY1", "VALUE1");
        ImmutableMap<String, Map<String, String>> catalogProperties = ImmutableMap.of("CATALOG", ImmutableMap.of("PROPERTY", "VALUE"));
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                defaultProperties,
                catalogProperties);

        assertProperties(defaultProperties, ImmutableMap.of(), catalogProperties, spec);
    }
}
