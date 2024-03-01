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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestDruidClient
{
    @Test
    public void testDruidRequestBodyToJson()
    {
        DruidClient.DruidRequestBody requestBody = new DruidClient.DruidRequestBody(
                "select \"city.name\" from \"geo-location\"",
                "arrayLines",
                false);
        assertEquals("{\n" +
                "  \"query\" : \"select \\\"city.name\\\" from \\\"geo-location\\\"\",\n" +
                "  \"resultFormat\" : \"arrayLines\",\n" +
                "  \"queryHeader\" : false\n" +
                "}",
                requestBody.toJson());
    }
}
