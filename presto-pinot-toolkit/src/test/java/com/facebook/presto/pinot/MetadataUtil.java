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
package com.facebook.presto.pinot;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    public static final JsonCodec<Map<String, List<PinotTable>>> CATALOG_CODEC;
    public static final JsonCodec<PinotTable> TABLE_CODEC;
    public static final JsonCodec<PinotColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<PinotClusterInfoFetcher.GetTables> TABLES_JSON_CODEC;
    public static final JsonCodec<PinotClusterInfoFetcher.BrokersForTable> BROKERS_FOR_TABLE_JSON_CODEC;
    public static final JsonCodec<PinotClusterInfoFetcher.Instance> INSTANCE_JSON_CODEC;
    public static final JsonCodec<PinotClusterInfoFetcher.RoutingTables> ROUTING_TABLES_JSON_CODEC;
    public static final JsonCodec<PinotClusterInfoFetcher.RoutingTablesV2> ROUTING_TABLES_V2_JSON_CODEC;
    public static final JsonCodec<PinotClusterInfoFetcher.TimeBoundary> TIME_BOUNDARY_JSON_CODEC;

    private MetadataUtil()
    {
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        public TestingTypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            return requireNonNull(type, "Unknown type " + value);
        }
    }

    static {
        JsonObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TestingTypeDeserializer(createTestFunctionAndTypeManager())));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        CATALOG_CODEC = codecFactory.mapJsonCodec(String.class, listJsonCodec(PinotTable.class));
        TABLE_CODEC = codecFactory.jsonCodec(PinotTable.class);
        COLUMN_CODEC = codecFactory.jsonCodec(PinotColumnHandle.class);
        TABLES_JSON_CODEC = codecFactory.jsonCodec(PinotClusterInfoFetcher.GetTables.class);
        BROKERS_FOR_TABLE_JSON_CODEC = codecFactory.jsonCodec(PinotClusterInfoFetcher.BrokersForTable.class);
        INSTANCE_JSON_CODEC = codecFactory.jsonCodec(PinotClusterInfoFetcher.Instance.class);
        ROUTING_TABLES_JSON_CODEC = codecFactory.jsonCodec(PinotClusterInfoFetcher.RoutingTables.class);
        ROUTING_TABLES_V2_JSON_CODEC = codecFactory.jsonCodec(PinotClusterInfoFetcher.RoutingTablesV2.class);
        TIME_BOUNDARY_JSON_CODEC = codecFactory.jsonCodec(PinotClusterInfoFetcher.TimeBoundary.class);
    }
}
