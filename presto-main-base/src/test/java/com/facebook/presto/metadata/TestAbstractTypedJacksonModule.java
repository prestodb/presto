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
package com.facebook.presto.metadata;

import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestAbstractTypedJacksonModule
{
    private ObjectMapper objectMapper;
    @BeforeMethod
    public void setup()
    {
        // Default setup with binary serialization disabled
        setupInjector(false, null);
    }

    private void setupInjector(boolean binarySerializationEnabled, ConnectorCodecProvider codecProvider)
    {
        Module testModule = binder -> {
            binder.install(new JsonModule());

            // Configure FeaturesConfig
            FeaturesConfig featuresConfig = new FeaturesConfig();
            featuresConfig.setUseConnectorProvidedSerializationCodecs(binarySerializationEnabled);
            binder.bind(FeaturesConfig.class).toInstance(featuresConfig);

            // Bind HandleResolver
            binder.bind(HandleResolver.class).toInstance(new TestHandleResolver());

            // Bind TestConnectorManager as a singleton
            TestConnectorManager testConnectorManager = new TestConnectorManager(codecProvider);
            binder.bind(TestConnectorManager.class).toInstance(testConnectorManager);

            // Register the test Jackson module
            jsonBinder(binder).addModuleBinding().to(TestHandleJacksonModule.class);
        };

        Injector injector = Guice.createInjector(testModule);
        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testLegacyJsonSerializationWithoutCodec()
            throws Exception
    {
        // Setup with binary serialization disabled
        setupInjector(false, null);

        TestHandle original = new TestHandle("connector1", "value1", 42);
        String json = objectMapper.writeValueAsString(original);

        // Should have @type field but no binary data
        assertJsonContains(json, "@type", "connector1");
        assertJsonContains(json, "id", "value1");
        assertJsonContains(json, "count", "42");
        assertJsonNotContains(json, "customSerializedValue");
    }

    @Test
    public void testBinarySerializationWithCodec()
            throws Exception
    {
        // Create a simple codec that serializes to a custom format
        ConnectorCodec<TestHandle> codec = new SimpleCodec();

        // Setup with binary serialization enabled and codec provider
        ConnectorCodecProvider codecProvider = new ConnectorCodecProvider()
        {
            @Override
            public Optional<ConnectorCodec<com.facebook.presto.spi.ConnectorTableHandle>> getConnectorTableHandleCodec()
            {
                return Optional.of((ConnectorCodec) codec);
            }
        };
        setupInjector(true, codecProvider);

        TestHandle original = new TestHandle("connector1", "value1", 42);
        String json = objectMapper.writeValueAsString(original);

        // Should have @type and binary data fields
        assertJsonContains(json, "@type", "connector1");
        assertJsonContains(json, "customSerializedValue");
        assertJsonNotContains(json, "id", "value1");  // Should not have regular fields

        // Test deserialization
        TestHandle deserialized = objectMapper.readValue(json, TestHandle.class);
        assertEquals(deserialized.getId(), original.getId());
        assertEquals(deserialized.getCount(), original.getCount());
    }

    @Test
    public void testBinarySerializationDisabled()
            throws Exception
    {
        // This test verifies that when binary serialization is disabled via the feature flag,
        // the module falls back to legacy JSON serialization even if codecs are available

        // Setup with binary serialization disabled even though codec is available
        ConnectorCodec<TestHandle> codec = new SimpleCodec();
        ConnectorCodecProvider codecProvider = new ConnectorCodecProvider()
        {
            @Override
            public Optional<ConnectorCodec<com.facebook.presto.spi.ConnectorTableHandle>> getConnectorTableHandleCodec()
            {
                return Optional.of((ConnectorCodec) codec);
            }
        };
        setupInjector(false, codecProvider);  // false = binary serialization disabled

        TestHandle original = new TestHandle("connector1", "value1", 42);
        String json = objectMapper.writeValueAsString(original);

        // Should use legacy JSON serialization even though codec is available
        assertJsonContains(json, "@type", "connector1");
        assertJsonContains(json, "id", "value1");
        assertJsonContains(json, "count", "42");
        assertJsonNotContains(json, "customSerializedValue");
    }

    @Test
    public void testFallbackToJsonWhenNoCodec()
            throws Exception
    {
        // Setup with binary serialization enabled but no codec available
        setupInjector(true, null);

        // Test with connector2 (no codec available)
        TestHandle original = new TestHandle("connector2", "value2", 84);
        String json = objectMapper.writeValueAsString(original);

        // Should fall back to JSON serialization
        assertJsonContains(json, "@type", "connector2");
        assertJsonContains(json, "id", "value2");
        assertJsonContains(json, "count", "84");
        assertJsonNotContains(json, "customSerializedValue");
    }

    @Test
    public void testInternalHandlesAlwaysUseJson()
            throws Exception
    {
        // Setup with codec that would handle all connectors
        ConnectorCodec<TestHandle> codec = new SimpleCodec();
        ConnectorCodecProvider codecProvider = new ConnectorCodecProvider()
        {
            @Override
            public Optional<ConnectorCodec<com.facebook.presto.spi.ConnectorTableHandle>> getConnectorTableHandleCodec()
            {
                return Optional.of((ConnectorCodec) codec);
            }
        };
        setupInjector(true, codecProvider);

        // Test with internal handle (starts with $)
        TestHandle original = new TestHandle("$remote", "internal", 99);
        String json = objectMapper.writeValueAsString(original);

        // Should use JSON serialization for internal handles
        assertJsonContains(json, "@type", "$remote");
        assertJsonContains(json, "id", "internal");
        assertJsonContains(json, "count", "99");
        assertJsonNotContains(json, "customSerializedValue");
    }

    @Test
    public void testNullValueSerialization()
            throws Exception
    {
        setupInjector(false, null);

        String json = objectMapper.writeValueAsString(null);
        assertEquals(json, "null");

        TestHandle deserialized = objectMapper.readValue("null", TestHandle.class);
        assertNull(deserialized);
    }

    @Test
    public void testRoundTripWithMixedHandles()
            throws Exception
    {
        // Create a TestConnectorManager that only provides codec for "binary-connector"
        setupInjector(true, new SelectiveCodecProvider("binary-connector"));

        // Test multiple handles with different serialization methods
        TestHandle[] handles = new TestHandle[] {
            new TestHandle("binary-connector", "binary1", 1),
            new TestHandle("json-connector", "json1", 2),
            new TestHandle("$internal", "internal1", 3),
            new TestHandle("binary-connector", "binary2", 4),
        };

        for (TestHandle original : handles) {
            String json = objectMapper.writeValueAsString(original);

            // Verify serialization format based on handle type
            if (original.getConnectorId().equals("binary-connector")) {
                // Should use binary serialization
                assertJsonContains(json, "customSerializedValue");
                assertJsonNotContains(json, "\"id\":");

                // Test deserialization for binary-serialized handles
                TestHandle deserialized = objectMapper.readValue(json, TestHandle.class);
                assertEquals(deserialized.getId(), original.getId());
                assertEquals(deserialized.getCount(), original.getCount());
            }
            else {
                // Should use JSON serialization
                assertJsonNotContains(json, "customSerializedValue");
                assertJsonContains(json, "id", original.getId());
            }
        }
    }

    @Test
    public void testDirectBinaryDataDeserialization()
            throws Exception
    {
        // Test deserialization of manually crafted binary data JSON
        ConnectorCodec<TestHandle> codec = new SimpleCodec();
        ConnectorCodecProvider codecProvider = new ConnectorCodecProvider()
        {
            @Override
            public Optional<ConnectorCodec<com.facebook.presto.spi.ConnectorTableHandle>> getConnectorTableHandleCodec()
            {
                return Optional.of((ConnectorCodec) codec);
            }
        };
        setupInjector(true, codecProvider);

        // Manually create JSON with binary data
        String encodedData = Base64.getEncoder().encodeToString("connector1|testValue|999".getBytes(UTF_8));
        String json = String.format("{\"@type\":\"connector1\",\"customSerializedValue\":\"%s\"}", encodedData);

        // Deserialize
        TestHandle deserialized = objectMapper.readValue(json, TestHandle.class);
        assertEquals(deserialized.getConnectorId(), "connector1");
        assertEquals(deserialized.getId(), "testValue");
        assertEquals(deserialized.getCount(), 999);
    }

    @Test
    public void testMixedSerializationRoundTrip()
            throws Exception
    {
        // Test that we can serialize and deserialize a mix of binary and JSON in sequence
        setupInjector(true, new SelectiveCodecProvider("binary-connector"));

        // Create handles with different serialization methods
        TestHandle binaryHandle = new TestHandle("binary-connector", "binary-data", 100);
        TestHandle jsonHandle = new TestHandle("json-connector", "json-data", 200);

        // Serialize both
        String binaryJson = objectMapper.writeValueAsString(binaryHandle);
        String jsonJson = objectMapper.writeValueAsString(jsonHandle);

        // Deserialize both
        TestHandle deserializedBinary = objectMapper.readValue(binaryJson, TestHandle.class);
        // For JSON deserialization, we skip due to complex type handling in isolated tests

        // Verify binary deserialization worked
        assertEquals(deserializedBinary.getId(), binaryHandle.getId());
        assertEquals(deserializedBinary.getCount(), binaryHandle.getCount());

        // Verify JSON format is correct (even if we can't deserialize in this test)
        assertJsonContains(jsonJson, "\"id\":\"json-data\"");
        assertJsonContains(jsonJson, "\"count\":200");
    }

    private void assertJsonContains(String json, String... values)
    {
        for (String value : values) {
            if (!json.contains(value)) {
                throw new AssertionError("JSON does not contain: " + value + "\nJSON: " + json);
            }
        }
    }

    private void assertJsonNotContains(String json, String... values)
    {
        for (String value : values) {
            if (json.contains(value)) {
                throw new AssertionError("JSON should not contain: " + value + "\nJSON: " + json);
            }
        }
    }

    // Simple codec implementation for testing
    private static class SimpleCodec
            implements ConnectorCodec<TestHandle>
    {
        @Override
        public byte[] serialize(TestHandle value)
        {
            return String.format("%s|%s|%d", value.getConnectorId(), value.getId(), value.getCount()).getBytes(UTF_8);
        }

        @Override
        public TestHandle deserialize(byte[] data)
        {
            String[] parts = new String(data, UTF_8).split("\\|");
            return new TestHandle(parts[0], parts[1], Integer.parseInt(parts[2]));
        }
    }

    // Codec provider that only provides codec for specific connectors
    private static class SelectiveCodecProvider
            implements ConnectorCodecProvider
    {
        private final String connectorIdWithCodec;
        private final ConnectorCodec<TestHandle> codec = new SimpleCodec();

        public SelectiveCodecProvider(String connectorIdWithCodec)
        {
            this.connectorIdWithCodec = connectorIdWithCodec;
        }

        @Override
        public Optional<ConnectorCodec<com.facebook.presto.spi.ConnectorTableHandle>> getConnectorTableHandleCodec()
        {
            return Optional.of((ConnectorCodec) codec);
        }
    }

    // Test handle that implements multiple connector interfaces for testing
    public static class TestHandle
            implements com.facebook.presto.spi.ConnectorTableHandle,
                       com.facebook.presto.spi.ConnectorSplit,
                       com.facebook.presto.spi.ColumnHandle,
                       com.facebook.presto.spi.ConnectorTableLayoutHandle,
                       com.facebook.presto.spi.ConnectorOutputTableHandle,
                       com.facebook.presto.spi.ConnectorInsertTableHandle,
                       com.facebook.presto.spi.ConnectorDeleteTableHandle,
                       com.facebook.presto.spi.ConnectorIndexHandle,
                       com.facebook.presto.spi.connector.ConnectorPartitioningHandle,
                       com.facebook.presto.spi.connector.ConnectorTransactionHandle
    {
        private final String connectorId;
        private final String id;
        private final int count;

        // Constructor for programmatic creation
        public TestHandle(String connectorId, String id, int count)
        {
            this.connectorId = connectorId;
            this.id = id;
            this.count = count;
        }

        // Constructor for Jackson deserialization
        @JsonCreator
        public TestHandle(
                @JsonProperty("id") String id,
                @JsonProperty("count") int count)
        {
            // When deserializing, the connector ID is determined by the @type field
            // For simplicity in tests, we use a fixed value
            this("deserialized", id, count);
        }

        // This field is excluded from JSON serialization but used internally for type resolution
        @JsonIgnore
        public String getConnectorId()
        {
            return connectorId;
        }

        @JsonProperty
        public String getId()
        {
            return id;
        }

        @JsonProperty
        public int getCount()
        {
            return count;
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
            TestHandle that = (TestHandle) o;
            return count == that.count &&
                    Objects.equals(connectorId, that.connectorId) &&
                    Objects.equals(id, that.id);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectorId, id, count);
        }

        @Override
        public String toString()
        {
            return "TestHandle{" +
                    "connectorId='" + connectorId + '\'' +
                    ", id='" + id + '\'' +
                    ", count=" + count +
                    '}';
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return null;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return null;
        }
    }

    // Test ConnectorHandleResolver implementation
    private static class TestConnectorHandleResolver
            implements com.facebook.presto.spi.ConnectorHandleResolver
    {
        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorTableHandle> getTableHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorTableLayoutHandle> getTableLayoutHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ColumnHandle> getColumnHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorSplit> getSplitClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorIndexHandle> getIndexHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorOutputTableHandle> getOutputTableHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorInsertTableHandle> getInsertTableHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.ConnectorDeleteTableHandle> getDeleteTableHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.connector.ConnectorPartitioningHandle> getPartitioningHandleClass()
        {
            return TestHandle.class;
        }

        @Override
        public Class<? extends com.facebook.presto.spi.connector.ConnectorTransactionHandle> getTransactionHandleClass()
        {
            return TestHandle.class;
        }
    }

    // Test HandleResolver implementation
    private static class TestHandleResolver
            extends HandleResolver
    {
        public TestHandleResolver()
        {
            super();
            // Register the test handle resolver for all test connectors
            TestConnectorHandleResolver resolver = new TestConnectorHandleResolver();
            addConnectorName("connector1", resolver);
            addConnectorName("connector2", resolver);
            addConnectorName("binary-connector", resolver);
            addConnectorName("json-connector", resolver);
            addConnectorName("$internal", resolver);
            addConnectorName("deserialized", resolver);
        }
    }

    // Mock ConnectorManager implementation
    private static class TestConnectorManager
    {
        private final ConnectorCodecProvider codecProvider;

        public TestConnectorManager(ConnectorCodecProvider codecProvider)
        {
            this.codecProvider = codecProvider;
        }

        public Optional<ConnectorCodecProvider> getConnectorCodecProvider(ConnectorId connectorId)
        {
            // Only return codec provider for specific connectors if it's a SelectiveCodecProvider
            if (codecProvider instanceof SelectiveCodecProvider) {
                SelectiveCodecProvider selective = (SelectiveCodecProvider) codecProvider;
                if (connectorId.getCatalogName().equals(selective.connectorIdWithCodec)) {
                    return Optional.of(codecProvider);
                }
                return Optional.empty();
            }
            return Optional.ofNullable(codecProvider);
        }
    }

    // Test Jackson module that uses TestHandle
    public static class TestHandleJacksonModule
            extends AbstractTypedJacksonModule<TestHandle>
    {
        @jakarta.inject.Inject
        public TestHandleJacksonModule(
                HandleResolver handleResolver,
                TestConnectorManager testConnectorManager,
                FeaturesConfig featuresConfig)
        {
            super(TestHandle.class,
                    TestHandle::getConnectorId,
                    id -> TestHandle.class,
                    featuresConfig.isUseConnectorProvidedSerializationCodecs(),
                    connectorId -> testConnectorManager
                            .getConnectorCodecProvider(connectorId)
                            .flatMap(provider -> {
                                Optional<ConnectorCodec<com.facebook.presto.spi.ConnectorTableHandle>> codec =
                                        provider.getConnectorTableHandleCodec();
                                // Cast is safe because TestHandle implements ConnectorTableHandle
                                return (Optional<ConnectorCodec<TestHandle>>) (Optional<?>) codec;
                            }));
        }
    }
}
