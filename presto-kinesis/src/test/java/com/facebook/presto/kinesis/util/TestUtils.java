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
package com.facebook.presto.kinesis.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;

import com.facebook.presto.kinesis.KinesisPlugin;
import com.facebook.presto.kinesis.KinesisStreamDescription;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

public class TestUtils
{
    private TestUtils() {}

    public static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public static void installKinesisPlugin(EmbeddedKinesisStream embeddedKinesisStream, QueryRunner queryRunner, Map<SchemaTableName, KinesisStreamDescription> streamDescriptions, String accessKey, String secretKey)
    {
        KinesisPlugin kinesisPlugin = new KinesisPlugin();
        kinesisPlugin.setTableDescriptionSupplier(Suppliers.ofInstance(streamDescriptions));
        queryRunner.installPlugin(kinesisPlugin);

        Map<String, String> kinesisConfig = ImmutableMap.of(
                    "kinesis.table-names", Joiner.on(",").join(streamDescriptions.keySet()),
                    "kinesis.default-schema", "default",
                    "kinesis.access-key", accessKey,
                    "kinesis.secret-key", secretKey);
        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);
    }

    public static Map.Entry<SchemaTableName, KinesisStreamDescription> createEmptyStreamDescription(String streamName, SchemaTableName schemaTableName)
    {
        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new KinesisStreamDescription(schemaTableName.getTableName(), schemaTableName.getSchemaName(), streamName, null));
    }
}
