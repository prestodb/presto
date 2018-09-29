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
package com.facebook.presto.kafkastream;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class KafkaMetadataClient
{
    /**
     * SchemaName -> (KafkaConfigBean)
     */
    private final Supplier<Map<String, KafkaConfigBean>> supplier;

    @Inject
    public KafkaMetadataClient(KafkaConfig config,
            JsonCodec<Map<String, KafkaConfigBean>> catalogCodec)
            throws IOException
    {
        requireNonNull("config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        supplier = Suppliers.memoize(schemasSupplier(catalogCodec,
                config.getMetadata()));
    }

    private static Supplier<Map<String, KafkaConfigBean>> schemasSupplier(
            final JsonCodec<Map<String, KafkaConfigBean>> catalogCodec, final String metadata)
    {
        return () -> {
            try {
                return lookupSchemas(metadata, catalogCodec);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private static Map<String, KafkaConfigBean> lookupSchemas(String metadataJson,
            JsonCodec<Map<String, KafkaConfigBean>> catalogCodec)
            throws IOException
    {
        Map<String, KafkaConfigBean> catalog = catalogCodec.fromJson(metadataJson);
        return ImmutableMap.copyOf(catalog);
    }

    public Map<String, KafkaConfigBean> getKafkaConfigBeanMap()
    {
        return supplier.get();
    }

    public KafkaConfigBean getKafkaConfigBean(String schema)
    {
        return supplier.get().get(schema);
    }

    public Set<String> getSchemaNames()
    {
        return getKafkaConfigBeanMap().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        KafkaConfigBean kafkaConfigBean = getKafkaConfigBean(schema);
        if (kafkaConfigBean == null) {
            return ImmutableSet.of();
        }
        return kafkaConfigBean.getKafkaTableMap().keySet();
    }

    public KafkaTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        KafkaConfigBean kafkaConfigBean = getKafkaConfigBean(schema);
        if (kafkaConfigBean == null) {
            return null;
        }
        return kafkaConfigBean.getKafkaTableMap().get(tableName);
    }
}
