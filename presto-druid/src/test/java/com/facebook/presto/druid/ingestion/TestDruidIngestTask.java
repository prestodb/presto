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
package com.facebook.presto.druid.ingestion;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;

public class TestDruidIngestTask
{
    @Test
    public void testDruidIngestTaskToJson()
    {
        DruidIngestTask ingestTask = new DruidIngestTask.Builder()
                .withDataSource("test_table_name")
                .withInputSource(new Path("file://test_path"), Collections.emptyList())
                .withTimestampColumn("__time")
                .withDimensions(ImmutableList.of(
                        new DruidIngestTask.DruidIngestDimension("string", "__time"),
                        new DruidIngestTask.DruidIngestDimension("double", "test_double_column")))
                .withAppendToExisting(true)
                .build();
        assertEquals(ingestTask.toJson(), "{\n" +
                "  \"type\" : \"index_parallel\",\n" +
                "  \"spec\" : {\n" +
                "    \"dataSchema\" : {\n" +
                "      \"dataSource\" : \"test_table_name\",\n" +
                "      \"timestampSpec\" : {\n" +
                "        \"column\" : \"__time\"\n" +
                "      },\n" +
                "      \"dimensionsSpec\" : {\n" +
                "        \"dimensions\" : [ {\n" +
                "          \"type\" : \"string\",\n" +
                "          \"name\" : \"__time\"\n" +
                "        }, {\n" +
                "          \"type\" : \"double\",\n" +
                "          \"name\" : \"test_double_column\"\n" +
                "        } ]\n" +
                "      }\n" +
                "    },\n" +
                "    \"ioConfig\" : {\n" +
                "      \"type\" : \"index_parallel\",\n" +
                "      \"inputSource\" : {\n" +
                "        \"type\" : \"local\",\n" +
                "        \"baseDir\" : \"file://test_path\",\n" +
                "        \"filter\" : \"*.json.gz\"\n" +
                "      },\n" +
                "      \"inputFormat\" : {\n" +
                "        \"type\" : \"json\"\n" +
                "      },\n" +
                "      \"appendToExisting\" : true\n" +
                "    }\n" +
                "  }\n" +
                "}");
    }
}
