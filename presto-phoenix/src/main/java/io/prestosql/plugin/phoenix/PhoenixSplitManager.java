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
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.shaded.com.google.common.base.Preconditions;
import org.apache.phoenix.shaded.com.google.common.collect.Lists;
import org.apache.phoenix.util.PhoenixRuntime;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static java.util.Objects.requireNonNull;

public class PhoenixSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PhoenixSplitManager.class);

    private final PhoenixClient phoenixClient;
    private PhoenixMetadata phoenixMetadata;
    private final Map<String, HostAddress> hostCache = new HashMap<>();

    @Inject
    public PhoenixSplitManager(PhoenixClient phoenixClient, PhoenixMetadataFactory phoenixMetadataFactory)
    {
        this.phoenixClient = requireNonNull(phoenixClient, "client is null");
        this.phoenixMetadata = phoenixMetadataFactory.create();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PhoenixTableLayoutHandle layoutHandle = (PhoenixTableLayoutHandle) layout;
        PhoenixTableHandle handle = layoutHandle.getTable();
        try (PhoenixConnection connection = phoenixClient.getConnection()) {
            String inputQuery = phoenixClient.buildSql(connection,
                    handle.getCatalogName(),
                    handle.getSchemaName(),
                    handle.getTableName(),
                    layoutHandle.getDesiredColumns(),
                    layoutHandle.getTupleDomain(),
                    phoenixMetadata.getColumns(handle, false));

            return new FixedSplitSource(getHadoopInputSplits(inputQuery).stream().map(split -> (PhoenixInputSplit) split).map(split -> {
                List<HostAddress> addresses = getSplitAddresses(split);

                return new PhoenixSplit(
                        phoenixClient.getConnectorId(),
                        handle.getCatalogName(),
                        handle.getSchemaName(),
                        handle.getTableName(),
                        layoutHandle.getTupleDomain(),
                        addresses,
                        new WrappedPhoenixInputSplit(split));
            }).collect(Collectors.toList()));
        }
        catch (IOException | InterruptedException | SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    private List<HostAddress> getSplitAddresses(PhoenixInputSplit split)
    {
        List<HostAddress> addresses;
        try {
            String hostName = split.getLocations()[0];
            HostAddress address = hostCache.get(hostName);
            if (address == null) {
                address = HostAddress.fromString(hostName);
                hostCache.put(hostName, address);
            }
            addresses = ImmutableList.of(address);
        }
        catch (Exception e) {
            log.warn("Failed to get split host addresses, will proceed without locality");
            addresses = ImmutableList.of();
        }
        return addresses;
    }

    // use Phoenix MR framework to get splits
    private List<InputSplit> getHadoopInputSplits(String inputQuery)
            throws SQLException, IOException, InterruptedException
    {
        Configuration conf = new Configuration();
        phoenixClient.setJobQueryConfig(inputQuery, conf);
        Job job = Job.getInstance(conf);
        List<InputSplit> splits = getSplits(job);
        return splits;
    }

    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context, configuration);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(queryPlan, allSplits, configuration);
        return splits;
    }

    // mostly copied from PhoenixInputFormat, but without the region size calculations
    private List<InputSplit> generateSplits(final QueryPlan qplan, final List<KeyRange> splits, Configuration config)
            throws IOException
    {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);

        try (org.apache.hadoop.hbase.client.Connection connection =
                HBaseFactoryProvider.getHConnectionFactory().createConnection(config)) {
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(qplan
                    .getTableRef().getTable().getPhysicalName().toString()));
            long regionSize = -1;
            final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
            for (List<Scan> scans : qplan.getScans()) {
                // Get the region location
                HRegionLocation location = regionLocator.getRegionLocation(
                        scans.get(0).getStartRow(),
                        false);
                String regionLocation = location.getHostname();

                // Generate splits based off statistics, or just region splits?
                boolean splitByStats = PhoenixConfigurationUtil.getSplitByStats(config);

                if (splitByStats) {
                    for (Scan aScan : scans) {
                        if (log.isDebugEnabled()) {
                            log.debug("Split for  scan : " + aScan + "with scanAttribute : " + aScan
                                    .getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : [" +
                                    aScan.getCaching() + ", " + aScan.getCacheBlocks() + ", " + aScan
                                    .getBatch() + "] and  regionLocation : " + regionLocation);
                        }
                        psplits.add(new PhoenixInputSplit(Collections.singletonList(aScan), regionSize, regionLocation));
                    }
                }
                else {
                    if (log.isDebugEnabled()) {
                        log.debug("Scan count[" + scans.size() + "] : " + Bytes.toStringBinary(scans
                                .get(0).getStartRow()) + " ~ " + Bytes.toStringBinary(scans.get(scans
                                .size() - 1).getStopRow()));
                        log.debug("First scan : " + scans.get(0) + "with scanAttribute : " + scans
                                .get(0).getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : " +
                                "[" + scans.get(0).getCaching() + ", " + scans.get(0).getCacheBlocks()
                                + ", " + scans.get(0).getBatch() + "] and  regionLocation : " +
                                regionLocation);

                        for (int i = 0, limit = scans.size(); i < limit; i++) {
                            log.debug("EXPECTED_UPPER_REGION_KEY[" + i + "] : " + Bytes
                                    .toStringBinary(scans.get(i).getAttribute
                                            (BaseScannerRegionObserver.EXPECTED_UPPER_REGION_KEY)));
                        }
                    }
                    psplits.add(new PhoenixInputSplit(scans, regionSize, regionLocation));
                }
            }
            return psplits;
        }
    }

    private QueryPlan getQueryPlan(final JobContext context, final Configuration configuration)
    {
        Preconditions.checkNotNull(context);
        try {
            final String txnScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
            final String currentScnValue = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            final String tenantId = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
            final Properties overridingProps = new Properties();
            if (txnScnValue == null && currentScnValue != null) {
                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
            }
            if (tenantId != null && configuration.get(PhoenixRuntime.TENANT_ID_ATTRIB) == null) {
                overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            }
            try (final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
                    final Statement statement = connection.createStatement()) {
                String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
                Preconditions.checkNotNull(selectStatement);

                final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
                // Optimize the query plan so that we potentially use secondary indexes
                final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
                final Scan scan = queryPlan.getContext().getScan();

                // since we can't set a scn on connections with txn set TX_SCN attribute so that the max time range is set by BaseScannerRegionObserver
                if (txnScnValue != null) {
                    scan.setAttribute(BaseScannerRegionObserver.TX_SCN, Bytes.toBytes(Long.valueOf(txnScnValue)));
                }

                // setting the snapshot configuration
                String snapshotName = configuration.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
                if (snapshotName != null) {
                    PhoenixConfigurationUtil.setSnapshotNameKey(queryPlan.getContext().getConnection()
                            .getQueryServices().getConfiguration(), snapshotName);
                }

                // Initialize the query plan so it sets up the parallel scans
                queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
                return queryPlan;
            }
        }
        catch (Exception exception) {
            log.error(String.format("Failed to get the query plan with error [%s]",
                    exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }
}
