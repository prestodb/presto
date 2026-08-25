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
package com.facebook.presto.iceberg.rest;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.PrestoRESTFileIO;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;

import java.util.Map;
import java.util.function.Function;

public class PrestoRestCatalog
        extends RESTCatalog
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorSession session;

    public PrestoRestCatalog(SessionCatalog.SessionContext sessionContext,
                             Function<Map<String, String>, RESTClient> clientBuilder,
                             HdfsEnvironment hdfsEnvironment, ConnectorSession session)
    {
        super(sessionContext, clientBuilder);
        this.hdfsEnvironment = hdfsEnvironment;
        this.session = session;
    }

    @Override
    public Table loadTable(TableIdentifier ident)
    {
        Table table = super.loadTable(ident);
        if (table.io() instanceof PrestoRESTFileIO) {
            ((PrestoRESTFileIO) table.io()).setHdfsEnvironmentAndContext(hdfsEnvironment, new HdfsContext(session));
        }
        return table;
    }
}
