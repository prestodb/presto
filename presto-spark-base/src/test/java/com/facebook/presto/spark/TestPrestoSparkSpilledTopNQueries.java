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

import java.util.HashMap;

import static com.facebook.presto.spark.PrestoSparkQueryRunner.createSpilledHivePrestoSparkQueryRunner;
import static io.airlift.tpch.TpchTable.getTables;

public class TestPrestoSparkSpilledTopNQueries
        extends TestPrestoSparkTopNQueries
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        HashMap<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("experimental.topn-spill-enabled", "true");
        additionalProperties.put("experimental.spiller.single-stream-spiller-choice", "TEMP_STORAGE");
        additionalProperties.put("experimental.memory-revoking-threshold", "0.0"); // revoke always
        additionalProperties.put("experimental.memory-revoking-target", "0.0");
        additionalProperties.put("query.max-memory", "110kB");

        return createSpilledHivePrestoSparkQueryRunner(getTables(), additionalProperties);
    }
}
