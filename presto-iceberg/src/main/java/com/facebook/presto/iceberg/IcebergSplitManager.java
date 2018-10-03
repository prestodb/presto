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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.expressions.Expression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeTranslator typeTranslator;
    private final TypeManager typeRegistry;

    @Inject
    public IcebergSplitManager(
            HdfsEnvironment hdfsEnvironment,
            TypeTranslator typeTranslator,
            TypeManager typeRegistry)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeTranslator = typeTranslator;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        final IcebergTableLayoutHandle tbl = (IcebergTableLayoutHandle) layout;
        final TupleDomain<HiveColumnHandle> predicates = tbl.getPredicates().getDomains()
                .map(m -> m.entrySet().stream().collect(Collectors.toMap((x) -> HiveColumnHandle.class.cast(x.getKey()), Map.Entry::getValue)))
                .map(m -> TupleDomain.withColumnDomains(m)).orElse(TupleDomain.none());
        final Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, tbl.getDatabase()), new Path("file:///tmp"));
        final Table icebergTable = getIcebergTable(tbl.getDatabase(), tbl.getTableName(), configuration);
        final Expression expression = ExpressionConverter.toIceberg(predicates, session);
        final TableScan tableScan = icebergTable.newScan().filter(expression);
        // TODO Use residual and move to scan tasks, Right now there is no way to propagate residual to presto.
        final IcebergSplitSource icebergSplitSource = new IcebergSplitSource(
                tbl.getDatabase(),
                tbl.getTableName(),
                tableScan.planFiles().iterator(),
                predicates,
                session,
                icebergTable.schema(),
                hdfsEnvironment,
                typeTranslator,
                typeRegistry,
                tbl.getNameToColumnHandle());
        return new ClassLoaderSafeConnectorSplitSource(icebergSplitSource, Thread.currentThread().getContextClassLoader());
    }
}
