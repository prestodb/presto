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
package com.facebook.presto.elasticsearch.client;

import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.elasticsearch.client.ElasticsearchClient.extractAddress;
import static org.testng.Assert.assertEquals;

public class TestExtractAddress
{
    @Test
    public void test()
    {
        assertEquals(extractAddress("node/1.2.3.4:9200"), Optional.of("node:9200"));
        assertEquals(extractAddress("1.2.3.4:9200"), Optional.of("1.2.3.4:9200"));
        assertEquals(extractAddress("node/1.2.3.4:9200"), Optional.of("node:9200"));
        assertEquals(extractAddress("node/[fe80::1]:9200"), Optional.of("node:9200"));
        assertEquals(extractAddress("[fe80::1]:9200"), Optional.of("[fe80::1]:9200"));

        assertEquals(extractAddress(""), Optional.empty());
        assertEquals(extractAddress("node/1.2.3.4"), Optional.empty());
        assertEquals(extractAddress("node/1.2.3.4:xxxx"), Optional.empty());
        assertEquals(extractAddress("1.2.3.4:xxxx"), Optional.empty());
    }
}
