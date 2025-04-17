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
package com.facebook.presto.hive.authentication;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class HdfsKerberosConfig
{
    private String hdfsPrestoPrincipal;
    private String hdfsPrestoKeytab;

    @NotNull
    public String getHdfsPrestoPrincipal()
    {
        return hdfsPrestoPrincipal;
    }

    @Config("hive.hdfs.presto.principal")
    @ConfigDescription("Presto principal used to access HDFS")
    public HdfsKerberosConfig setHdfsPrestoPrincipal(String hdfsPrestoPrincipal)
    {
        this.hdfsPrestoPrincipal = hdfsPrestoPrincipal;
        return this;
    }

    @NotNull
    public String getHdfsPrestoKeytab()
    {
        return hdfsPrestoKeytab;
    }

    @Config("hive.hdfs.presto.keytab")
    @ConfigDescription("Presto keytab used to access HDFS")
    public HdfsKerberosConfig setHdfsPrestoKeytab(String hdfsPrestoKeytab)
    {
        this.hdfsPrestoKeytab = hdfsPrestoKeytab;
        return this;
    }
}
