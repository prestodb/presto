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
package com.facebook.presto.tests.functions;

import com.teradata.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.URL_FUNCTIONS;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class UrlFunctionsTests
        extends ProductTest
{
    private static final String URL = "https://prestodb.io:80/docs/current/connector/hive.html?param_name=param_value#hash";

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractFragmentExist()
    {
        assertThat(query(format("SELECT url_extract_fragment('%s')", URL))).containsExactly(row("hash"));
    }

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractHostExist()
    {
        assertThat(query(format("SELECT url_extract_host('%s')", URL))).containsExactly(row("prestodb.io"));
    }

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractParameterExist()
    {
        assertThat(query(format("SELECT url_extract_parameter('%s','param_name')", URL))).containsExactly(row("param_value"));
    }

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractPathExist()
    {
        assertThat(query(format("SELECT url_extract_path('%s')", URL))).containsExactly(row("/docs/current/connector/hive.html"));
    }

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractPortExist()
    {
        assertThat(query(format("SELECT url_extract_port('%s')", URL))).containsExactly(row(80L));
    }

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractProtocolExist()
    {
        assertThat(query(format("SELECT url_extract_protocol('%s')", URL))).containsExactly(row("https"));
    }

    @Test(groups = {URL_FUNCTIONS})
    public void testUrlExtractQueryExist()
    {
        assertThat(query(format("SELECT url_extract_query('%s')", URL))).containsExactly(row("param_name=param_value"));
    }
}
