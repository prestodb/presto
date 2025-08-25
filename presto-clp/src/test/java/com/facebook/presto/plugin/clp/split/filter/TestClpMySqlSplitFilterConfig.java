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
package com.facebook.presto.plugin.clp.split.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpMySqlSplitFilterConfig
{
    private String filterConfigPath;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-mysql-split-filter.json");
        if (resource == null) {
            throw new FileNotFoundException("test-mysql-split-filter.json not found in resources");
        }

        filterConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
    }

    @Test
    public void remapSplitFilterPushDownExpression()
    {
        ClpConfig config = new ClpConfig();
        config.setSplitFilterConfig(filterConfigPath);
        ClpMySqlSplitFilterProvider filterProvider = new ClpMySqlSplitFilterProvider(config);

        // Integer
        testRange(1234, 5678, filterProvider);
        testRange(-5678, -1234, filterProvider);

        // Decimal
        testRange(1234.001, 5678.999, filterProvider);
        testRange(-5678.999, -1234.001, filterProvider);

        // Scientific
        testRange("1.234E3", "5.678e3", filterProvider);
        testRange("-1.234e-3", "-5.678E-3", filterProvider);
    }

    private <T> void testRange(T lowerBound, T upperBound, ClpMySqlSplitFilterProvider filterProvider)
    {
        String splitFilterSql1 = format("(\"msg.timestamp\" > %s AND \"msg.timestamp\" < %s)", lowerBound, upperBound);
        String remappedSql1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", splitFilterSql1);
        assertEquals(remappedSql1, format("(end_timestamp > %s AND begin_timestamp < %s)", lowerBound, upperBound));

        String splitFilterSql2 = format("(\"msg.timestamp\" >= %s AND \"msg.timestamp\" <= %s)", lowerBound, upperBound);
        String remappedSql2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", splitFilterSql2);
        assertEquals(remappedSql2, format("(end_timestamp >= %s AND begin_timestamp <= %s)", lowerBound, upperBound));

        String splitFilterSql3 = format("(\"msg.timestamp\" > %s AND \"msg.timestamp\" <= %s)", lowerBound, upperBound);
        String remappedSql3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", splitFilterSql3);
        assertEquals(remappedSql3, format("(end_timestamp > %s AND begin_timestamp <= %s)", lowerBound, upperBound));

        String splitFilterSql4 = format("(\"msg.timestamp\" >= %s AND \"msg.timestamp\" < %s)", lowerBound, upperBound);
        String remappedSql4 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", splitFilterSql4);
        assertEquals(remappedSql4, format("(end_timestamp >= %s AND begin_timestamp < %s)", lowerBound, upperBound));

        String splitFilterSql5 = format("(\"msg.timestamp\" = %s)", lowerBound);
        String remappedSql5 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", splitFilterSql5);
        assertEquals(remappedSql5, format("((begin_timestamp <= %s AND end_timestamp >= %s))", lowerBound, lowerBound));
    }
}
