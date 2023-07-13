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
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

public class SampleUtil
{
    private SampleUtil() {}

    public static final TableIdentifier SAMPLE_TABLE_ID = toIcebergTableIdentifier("sample", "sample-table");

    public static HadoopCatalog getCatalogForSampleTable(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
    {
        Path tableLocation = new Path(icebergSource.location());
        HdfsContext ctx = new HdfsContext(session, prestoSchema, icebergSource.name(), icebergSource.location(), false);
        HadoopCatalog c = new HadoopCatalog();
        Map<String, String> props = new HashMap<>();
        c.setConf(env.getConfiguration(ctx, tableLocation));
        props.put(WAREHOUSE_LOCATION, tableLocation.toString());
        // initializing the catalog can be expensive. See if we can somehow store the catalog reference
        // or wrap a delegate catalog.
        c.initialize(tableLocation.getName(), props);
        return c;
    }

    public static boolean sampleTableExists(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
    {
        try (HadoopCatalog catalog = getCatalogForSampleTable(icebergSource, prestoSchema, env, session)) {
            return catalog.tableExists(SAMPLE_TABLE_ID);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, e);
        }
    }

    public static Table getSampleTableFromActual(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
    {
        try (HadoopCatalog catalog = getCatalogForSampleTable(icebergSource, prestoSchema, env, session)) {
            return catalog.loadTable(SAMPLE_TABLE_ID);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, e);
        }
    }
}
