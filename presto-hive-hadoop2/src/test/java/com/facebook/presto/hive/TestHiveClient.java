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
package com.facebook.presto.hive;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbill.DNS.Lookup;

@Test
public class TestHiveClient
        extends AbstractTestHiveClient
{
    @Parameters({"hive.hadoop2.metastoreHost", "hive.hadoop2.metastorePort", "hive.hadoop2.databaseName", "hive.hadoop2.timeZone", "dns.port"})
    @BeforeClass
    public void initialize(String host, int port, String databaseName, String timeZone, @Optional Integer dnsPort)
    {
        // Connecting to Hive cluster (e.g. dockerized) over SOCKS proxy requires using a DNS server, which resolves cluster IP addresses (e.g. hadoop-master).
        // The DNS server might listen on custom port. Therefore we need to configure the name resolver to use that port.
        if (dnsPort != null) {
            Lookup.getDefaultResolver().setPort(dnsPort);
        }

        setup(host, port, databaseName, timeZone);
    }
}
