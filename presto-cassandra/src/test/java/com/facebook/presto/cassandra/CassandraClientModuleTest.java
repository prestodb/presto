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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.Cluster;
import com.facebook.airlift.json.JsonCodec;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CassandraClientModuleTest
{
    private CassandraConnectorId ccId;

    private Cluster.Builder mockClusterBuilder;

    private CassandraClientConfig ccConfig;

    private JsonCodec<List<ExtraColumnMetadata>> mockExtraColumnMetadataCodec;

    @BeforeMethod
    public void setUp()
    {
        ccId = new CassandraConnectorId("test");
        mockClusterBuilder = Mockito.mock(Cluster.Builder.class, Answers.RETURNS_SELF);
        when(mockClusterBuilder.build()).thenReturn(mock(Cluster.class));
        ccConfig = new CassandraClientConfig();
        mockExtraColumnMetadataCodec = Mockito.mock(JsonCodec.class);
    }

    @Test
    public void testContactPointsOnly()
    {
        ccConfig.setContactPoints("127.0.0.1", "127.0.0.2", "127.0.0.3");
        CassandraClientModule.createCassandraSession(mockClusterBuilder, ccId, ccConfig, mockExtraColumnMetadataCodec);
        verify(mockClusterBuilder, times(3)).addContactPoint(anyString());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testContactPointsAndAstraBundleThrowError()
    {
        ccConfig.setContactPoints("127.0.0.1", "127.0.0.2", "127.0.0.3");
        String testData = "Astra secure connect bundle data..";
        ccConfig.setAstraSecureConnectBundlePath("/tmp/my_astra.csb");
        CassandraClientModule.createCassandraSession(mockClusterBuilder, ccId, ccConfig, mockExtraColumnMetadataCodec);
    }

    @Test
    public void astraBundleOnly()
    {
        String testData = "Astra secure connect bundle data..";
        ccConfig.setAstraSecureConnectBundlePath("/tmp/my_astra.csb");
        CassandraClientModule.createCassandraSession(mockClusterBuilder, ccId, ccConfig, mockExtraColumnMetadataCodec);
        verify(mockClusterBuilder).withCloudSecureConnectBundle(any(File.class));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNoContactPointsOrAstraBundle()
    {
        CassandraClientModule.createCassandraSession(mockClusterBuilder, ccId, ccConfig, mockExtraColumnMetadataCodec);
    }
}
