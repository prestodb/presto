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
package com.facebook.presto.plugin.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestBigQueryConnectorModule
{
    @Test
    public void testConfigurationOnly()
            throws Exception
    {
        String projectId = BigQueryConnectorModule.calculateBillingProjectId(Optional.of("pid"), Optional.empty());
        assertThat(projectId).isEqualTo("pid");
    }

    @Test
    public void testCredentialsOnly()
            throws Exception
    {
        String projectId = BigQueryConnectorModule.calculateBillingProjectId(Optional.empty(), credentials());
        assertThat(projectId).isEqualTo("presto-bq-credentials-test");
    }

    @Test
    public void testBothConfigurationAndCredentials()
            throws Exception
    {
        String projectId = BigQueryConnectorModule.calculateBillingProjectId(Optional.of("pid"), credentials());
        assertThat(projectId).isEqualTo("pid");
    }

    private Optional<Credentials> credentials()
            throws IOException
    {
        return Optional.of(GoogleCredentials.fromStream(getClass().getResourceAsStream("/test-account.json")));
    }
}
