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

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;

import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static io.airlift.tpch.TpchTable.getTables;

public class TestPrestoSparkSpilledAggregations
        extends TestPrestoSparkAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        ImmutableMap.Builder<String, String> configProperties = ImmutableMap.builder();
        configProperties.put("experimental.spill-enabled", "true");
        configProperties.put("experimental.join-spill-enabled", "true");
        configProperties.put("experimental.temp-storage-buffer-size", "1MB");
        configProperties.put("spark.memory-revoking-threshold", "0.0");
        configProperties.put("experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString());
        return createHivePrestoSparkQueryRunner(getTables(), configProperties.build());
    }
}
