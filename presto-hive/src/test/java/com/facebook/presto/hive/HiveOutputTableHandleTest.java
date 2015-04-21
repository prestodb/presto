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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class HiveOutputTableHandleTest
{
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);

    private final JsonCodec<HiveOutputTableHandle> codec = createJsonCodec();

    @Test
    public void testHiveOutputTableHandleRoundTrip()
    {
        HiveOutputTableHandle expected = new HiveOutputTableHandle(
                "client_value",
                "schema_name_value",
                "table_name_value",
                asList("c1", "c2", "c3"),
                asList(BIGINT, DOUBLE, VARCHAR),
                "owner",
                "/t_path",
                "/tmp_path",
                SESSION,
                DWRF,
                ImmutableMap.<String, String>builder().put("p1", "v1").put("p2", "v2").build(),
                ImmutableMap.<String, String>builder().put("p3", "v3").put("p4", "v4").build()
        );

        String json = codec.toJson(expected);
        HiveOutputTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getSchemaName(), expected.getSchemaName());
        assertEquals(actual.getTableName(), expected.getTableName());
        assertEquals(actual.getColumnNames(), expected.getColumnNames());
        assertEquals(actual.getColumnTypes(), expected.getColumnTypes());
        assertEquals(actual.getTableOwner(), expected.getTableOwner());
        assertEquals(actual.getTargetPath(), expected.getTargetPath());
        assertEquals(actual.getTemporaryPath(), expected.getTemporaryPath());
        assertEquals(actual.getTableParameters(), expected.getTableParameters());
        assertEquals(actual.getSerdeParameters(), expected.getSerdeParameters());
    }

    private JsonCodec<HiveOutputTableHandle> createJsonCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(singletonMap(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(objectMapperProvider, true).jsonCodec(HiveOutputTableHandle.class);
    }
}
