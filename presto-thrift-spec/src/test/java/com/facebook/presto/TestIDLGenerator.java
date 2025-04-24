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
package com.facebook.presto;

import com.facebook.drift.idl.generator.ThriftIdlGenerator;
import com.facebook.drift.idl.generator.ThriftIdlGeneratorConfig;
import com.facebook.presto.spi.security.SelectedRole;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.function.Consumer;

import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

/**
 * This class exists to force the creation of a jar for the presto-thrift-spec module.
 * This is needed to deploy the presto-thrift-spec module to Nexus.
 */
public class TestIDLGenerator
{
    @Test
    public void testGenerator()
            throws Exception
    {
        assertGenerated(SelectedRole.class, "distribution", ignored -> {});
    }

    private static void assertGenerated(Class<?> clazz, String name, Consumer<ThriftIdlGeneratorConfig.Builder> configConsumer)
            throws IOException
    {
        String expected = Resources.toString(getResource(format("expected/%s.txt", name)), UTF_8);

        ThriftIdlGeneratorConfig.Builder config = ThriftIdlGeneratorConfig.builder()
                .includes(ImmutableMap.of())
                .namespaces(ImmutableMap.of())
                .recursive(true);
        configConsumer.accept(config);

        ThriftIdlGenerator generator = new ThriftIdlGenerator(config.build());
        String idl = generator.generate(ImmutableList.of(clazz.getName()));

        assertEquals(idl, expected);
    }
}
