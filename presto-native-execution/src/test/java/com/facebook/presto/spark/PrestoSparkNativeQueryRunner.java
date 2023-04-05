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
package com.facebook.presto.spark;

import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.nativeworker.AbstractNativeRunner;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spark.PrestoSparkQueryRunner.METASTORE_CONTEXT;

public class PrestoSparkNativeQueryRunner
        extends AbstractNativeRunner
{
    private static final int AVAILABLE_CPU_COUNT = 4;

    public static PrestoSparkQueryRunner createPrestoSparkNativeQueryRunner(Map<String, String> additionalConfigProperties, Map<String, String> additionalSparkProperties)
    {
        String dataDirectory = System.getProperty("DATA_DIR");

        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        configBuilder.putAll(getNativeWorkerSystemProperties()).putAll(additionalConfigProperties);

        PrestoSparkQueryRunner queryRunner = new PrestoSparkQueryRunner(
                "hive",
                configBuilder.build(),
                getNativeWorkerHiveProperties(),
                additionalSparkProperties,
                Optional.ofNullable(dataDirectory).map(Paths::get),
                AVAILABLE_CPU_COUNT);

        ExtendedHiveMetastore metastore = queryRunner.getMetastore();
        if (!metastore.getDatabase(METASTORE_CONTEXT, "tpch").isPresent()) {
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject("tpch"));
        }
        return queryRunner;
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }
}
