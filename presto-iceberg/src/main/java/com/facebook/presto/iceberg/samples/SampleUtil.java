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
package com.facebook.presto.iceberg.samples;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.HashMap;

import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;

public class SampleUtil
{
    private SampleUtil() {}

    public static final String SAMPLE_TABLE_SUFFIX = "sample-table";

    public static Table getSampleTableFromActual(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
            throws IOException
    {
        Path tableLocation = new Path(icebergSource.location());
        HdfsContext ctx = new HdfsContext(session, prestoSchema, icebergSource.name(), icebergSource.location(), false);
        try (HadoopCatalog hadoopCatalog = new HadoopCatalog(env.getConfiguration(ctx, tableLocation), icebergSource.location())) {
            // initializing the catalog can be expensive. See if we can somehow store the catalog reference
            // or wrap a delegate catalog.
            hadoopCatalog.initialize(tableLocation.getName(), new HashMap<>());
            TableIdentifier id = toIcebergTableIdentifier("sample", SAMPLE_TABLE_SUFFIX);
            return hadoopCatalog.loadTable(id);
        }
    }
}
