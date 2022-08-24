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

import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.TimestampType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDruidBrokerPageSource
{
    private final ObjectMapper mapper = new ObjectMapper();

    @DataProvider(name = "gooddates")
    public Object[][] goodDates()
    {
        return new Object[][] {
                {"\"2011-12-03T10:15:30Z\"", 1322907330000L},
                {"\"2011-12-03T10:15:30Z\"", 1322907330000L},
                {"\"1970-01-01T00:00:00Z\"", 0L},
                {"\"1970-01-01T00:00:00\"", 0L},
                {"\"1970-01-01T00:00:00.435\"", 435L},
                {"\"1970-01-01T00:00:00.4356789\"", 435L},
                {"\"1970-01-01T00:00:00.43\"", 430L},
                {"\"1969-01-01\"", -31536000000L},
                {"\"1985\"", 473385600000L},
                {"\"85\"", -59484758400000L},
                {"\"2011-12-03T\"", 1322870400000L},
                {"\"2011-12-03\"", 1322870400000L},
                {"\"2011-12-03T10\"", 1322906400000L},
                {"\"15000-12-03T10\"", 411216170400000L},
        };
    }

    @DataProvider(name = "baddates") //
    public Object[][] badDates()
    {
        return new Object[][] {
                {"\"2011-12-45\""},
                {"\"not a date\""},
                {"\"2011-12-03T10:\""},
                {"\"2011-12-03T1234\""},
        };
    }

    @Test(dataProvider = "gooddates")
    public void testWriteTimestamp(String json, long expected)
            throws JsonProcessingException
    {
        JsonNode node = mapper.readTree(json);
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 1);
        DruidBrokerPageSource.writeValue(TimestampType.TIMESTAMP, blockBuilder, node);
        assertEquals(blockBuilder.getLong(0), expected);
    }

    @Test(dataProvider = "baddates")
    public void testWriteInvalidTimestamp(String json)
            throws JsonProcessingException
    {
        JsonNode node = mapper.readTree(json);
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 1);
        try {
            DruidBrokerPageSource.writeValue(TimestampType.TIMESTAMP, blockBuilder, node);
            fail("Allowed " + json);
        }
        catch (IllegalArgumentException ex) {
        }
    }
}
