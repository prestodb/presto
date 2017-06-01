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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestQueryRunner
{
    @Test
    public void testGivenHttpClientConfig()
    {
        ClientSession session = new ClientOptions().toClientSession();
        HttpClientConfig mockConfig = spy(new HttpClientConfig());
        when(mockConfig.getConnectTimeout()).thenReturn(new Duration(4, TimeUnit.SECONDS));
        QueryRunner runner = QueryRunner.create(session,
                Optional.<HostAndPort>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                Optional.<String>empty(),
                false,
                null,
                Optional.of(mockConfig)
                );

        // Configs are fetched from a given HttpClientConfig.
        verify(mockConfig, atLeast(1)).getConnectTimeout();
        verify(mockConfig, atLeast(1)).getRequestTimeout();
    }
}
