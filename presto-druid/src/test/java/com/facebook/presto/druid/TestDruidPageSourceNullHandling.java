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
package com.facebook.presto.druid;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertTrue;

public class TestDruidPageSourceNullHandling
{
    @Test
    public void testNullAndMissingColumns()
    {
        String jsonRows =
                "{\"region.Id\":1,\"city\":\"Boston\",\"fare\":10.0}\n" +
                        "{\"region.Id\":2,\"city\":null,\"fare\":20.0}\n" + // city column is having null value
                        "{\"region.Id\":3,\"fare\":30.0}\n" + // missing city column
                        "\n";

        ListMultimap<String, String> headers = ImmutableListMultimap.of(
                "Content-Type", "application/json");
        TestingResponse response = new TestingResponse(
                HttpStatus.OK,
                headers,
                jsonRows.getBytes(StandardCharsets.UTF_8));
        HttpClient httpClient = new TestingHttpClient(request -> response);

        DruidConfig druidConfig = new DruidConfig()
                .setDruidSchema("default")
                .setDruidCoordinatorUrl("http://localhost:8081")
                .setDruidBrokerUrl("http://localhost:8082");

        ImmutableList<ColumnHandle> columnHandles = ImmutableList.of(new DruidColumnHandle("region.Id", BIGINT),
                new DruidColumnHandle("city", VARCHAR),
                new DruidColumnHandle("fare", DOUBLE));

        DruidBrokerPageSource pageSource = new DruidBrokerPageSource(
                new DruidQueryGenerator.GeneratedDql("testTable", "SELECT region.Id, city, fare FROM test", true),
                columnHandles,
                new DruidClient(druidConfig, httpClient));

        Page page;
        boolean foundNull = false;
        boolean foundMissing = false;

        while ((page = pageSource.getNextPage()) != null) {
            Block cityBlock = page.getBlock(1);
            for (int i = 0; i < cityBlock.getPositionCount(); i++) {
                if (cityBlock.isNull(i)) {
                    if (i == 1) {
                        foundNull = true; // row with "city":null
                    }
                    if (i == 2) {
                        foundMissing = true; // row missing "city"
                    }
                }
            }
        }

        assertTrue(foundNull, "Expected null value in column 'city'");
        assertTrue(foundMissing, "Expected missing column to be treated as null");
    }
}
