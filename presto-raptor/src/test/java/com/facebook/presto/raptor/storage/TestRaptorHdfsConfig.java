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
package com.facebook.presto.raptor.storage;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.raptor.filesystem.RaptorHdfsConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestRaptorHdfsConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(RaptorHdfsConfig.class)
                .setSocksProxy(null)
                .setDfsTimeout(new Duration(60, TimeUnit.SECONDS))
                .setIpcPingInterval(new Duration(10, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setDomainSocketPath(null)
                .setTextMaxLineLength(new DataSize(100, Unit.MEGABYTE))
                .setResourceConfigFiles("")
                .setFileSystemMaxCacheSize(1000)
                .setHdfsWireEncryptionEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.thrift.client.socks-proxy", "localhost:1080")
                .put("hive.dfs.ipc-ping-interval", "34s")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.dfs.domain-socket-path", "/foo")
                .put("hive.text.max-line-length", "13MB")
                .put("hive.fs.cache.max-size", "1010")
                .put("hive.hdfs.wire-encryption.enabled", "true")
                .put("hive.config.resources", "a,b,c")
                .build();

        RaptorHdfsConfig expected = new RaptorHdfsConfig()
                .setSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setIpcPingInterval(new Duration(34, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setDomainSocketPath("/foo")
                .setTextMaxLineLength(new DataSize(13, Unit.MEGABYTE))
                .setFileSystemMaxCacheSize(1010)
                .setResourceConfigFiles(ImmutableList.of("a", "b", "c"))
                .setHdfsWireEncryptionEnabled(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
