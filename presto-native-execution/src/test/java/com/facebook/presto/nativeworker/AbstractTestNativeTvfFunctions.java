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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpRequestFilter;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.server.InternalAuthenticationManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.tvf.TVFProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tvf.NativeTVFProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public abstract class AbstractTestNativeTvfFunctions
        extends AbstractTestQueryFramework
{
    private static final String TVF_PROVIDER_NAME = "system";

    @Override
    protected void createTables()
    {
        createRegion((QueryRunner) getExpectedQueryRunner());
    }

    @Test
    public void testSequence()
    {
        assertQuery("SELECT * FROM TABLE(sequence( start => 20, stop => 100, step => 5))");
    }

    @Test
    public void testExcludeColumns()
    {
        assertQuery("SELECT * FROM TABLE(exclude_columns(input => TABLE(region), columns => DESCRIPTOR(regionkey, comment)))");
    }

    @Test
    public void testInternalAuthenticationFilterPresent()
    {
        TVFProvider tvfProvider =
                getQueryRunner().getMetadata().getFunctionAndTypeManager()
                        .getTvfProviders().get(
                                new ConnectorId(TVF_PROVIDER_NAME));
        checkArgument(tvfProvider instanceof NativeTVFProvider, "Expected  NativeTVFProvider but got  %s", tvfProvider);
        HttpClient httpClient = ((NativeTVFProvider) tvfProvider).getHttpClient();
        // check if filter present
        List<HttpRequestFilter> filters = ((JettyHttpClient) httpClient).getRequestFilters();

        InternalAuthenticationManager authenticationManager = filters.stream()
                .filter(InternalAuthenticationManager.class::isInstance)
                .map(InternalAuthenticationManager.class::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("InternalAuthenticationManager filter not found"));

        // Verify that the test shared secret is propagated all the way through
        assertTrue(authenticationManager.getSharedSecret().isPresent());
        assertEquals(authenticationManager.getSharedSecret().get(), "internal-shared-secret");
    }
}
