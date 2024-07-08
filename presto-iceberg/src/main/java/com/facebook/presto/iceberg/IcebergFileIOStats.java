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
package com.facebook.presto.iceberg;

import org.weakref.jmx.Managed;

import javax.inject.Inject;

public class IcebergFileIOStats
{
    @Inject
    private static PrestoInMemoryContentCacheManager inMemoryContentCacheManager;

    private static final String HDFS_FILE_IO = "com.facebook.presto.iceberg.HdfsFileIO";
    private static final String HADOOP_FILE_IO = "org.apache.iceberg.hadoop.HadoopFileIO";

    @Managed
    public String getHiveCatalogIOStats()
    {
        return inMemoryContentCacheManager.getCache().getIfPresent(HDFS_FILE_IO).stats().toString();
    }

    @Managed
    public String getHadoopCatalogIOStats()
    {
        return inMemoryContentCacheManager.getCache().getIfPresent(HADOOP_FILE_IO).stats().toString();
    }

    @Managed
    public String getNessieCatalogIOStats()
    {
        return inMemoryContentCacheManager.getCache().getIfPresent(HADOOP_FILE_IO).stats().toString();
    }

    @Managed
    public String getRestCatalogIOStats()
    {
        return inMemoryContentCacheManager.getCache().getIfPresent(HADOOP_FILE_IO).stats().toString();
    }
}
