package com.facebook.presto.plugin.hbase;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hbase.HbaseClient;
import com.facebook.presto.hbase.HbaseTableManager;
import com.facebook.presto.hbase.TabletSplitMetadata;
import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.hbase.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.mockito.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.AssertJUnit.assertEquals;


public class TestHbaseClient {

    @Mock
    private Connection connection;

    @Mock
    private ZooKeeperMetadataManager metaManager;

    @Mock
    private HbaseTableManager tableManager;

    @Mock
    private HbaseConfig hbaseConfig;

    @Mock
    private ConnectorSession session;

    @InjectMocks
    private HbaseClient hbaseClient;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void getTabletSplits_NoRowIdDomain_ReturnsFullTableScan() throws Exception {
        String schema = "testSchema";
        String table = "testTable";
        Optional<Domain> rowIdDomain = Optional.empty();
        List<HbaseColumnConstraint> constraints = ImmutableList.of();

        TableInputFormat tableInputFormat = Mockito.mock(TableInputFormat.class);
        TableSplit tableSplit = Mockito.mock(TableSplit.class);
        Mockito.when(tableSplit.getTable()).thenReturn(TableName.valueOf(schema, table));
        Mockito.when(tableSplit.getStartRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit.getEndRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit.getRegionLocation()).thenReturn("regionLocation");
        Mockito.when(tableSplit.getLength()).thenReturn(100L);

        Mockito.when(connection.getConfiguration()).thenReturn(new JobConf());
        Mockito.when(connection.getTable(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getRegionLocator(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getAdmin()).thenReturn(null);

        Mockito.when(tableInputFormat.getSplits(ArgumentMatchers.any(JobContext.class)))
                .thenReturn(ImmutableList.of(tableSplit));

        List<TabletSplitMetadata> tabletSplits = hbaseClient.getTabletSplits(session, schema, table, rowIdDomain, constraints);

        assertEquals(1, tabletSplits.size());
        TabletSplitMetadata splitMetadata = tabletSplits.get(0);
        assertEquals(schema, splitMetadata.getTableName());
        assertEquals(table, splitMetadata.getTableName());
    }

    @Test
    public void getTabletSplits_SingleRowIdRange_ReturnsSingleSplit() throws Exception {
        String schema = "testSchema";
        String table = "testTable";
        Type type = BIGINT;
        Optional<Domain> rowIdDomain = Optional.of(Domain.singleValue(type, 1L));
        List<HbaseColumnConstraint> constraints = ImmutableList.of();

        TableInputFormat tableInputFormat = Mockito.mock(TableInputFormat.class);
        TableSplit tableSplit = Mockito.mock(TableSplit.class);
        Mockito.when(tableSplit.getTable()).thenReturn(TableName.valueOf(schema, table));
        Mockito.when(tableSplit.getStartRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit.getEndRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit.getRegionLocation()).thenReturn("regionLocation");
        Mockito.when(tableSplit.getLength()).thenReturn(100L);

        Mockito.when(connection.getConfiguration()).thenReturn(new JobConf());
        Mockito.when(connection.getTable(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getRegionLocator(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getAdmin()).thenReturn(null);

        Mockito.when(tableInputFormat.getSplits(ArgumentMatchers.any(JobContext.class)))
                .thenReturn(ImmutableList.of(tableSplit));

        List<TabletSplitMetadata> tabletSplits = hbaseClient.getTabletSplits(session, schema, table, rowIdDomain, constraints);

        assertEquals(1, tabletSplits.size());
        TabletSplitMetadata splitMetadata = tabletSplits.get(0);
        assertEquals(schema, splitMetadata.getTableName());
        assertEquals(table, splitMetadata.getTableName());
    }

    @Test
    public void getTabletSplits_MultipleRowIdRanges_ReturnsMultipleSplits() throws Exception {
        String schema = "testSchema";
        String table = "testTable";
        Type type = BIGINT;
        Optional<Domain> rowIdDomain = Optional.of(Domain.multipleValues(type, ImmutableList.of(1L, 2L)));
        List<HbaseColumnConstraint> constraints = ImmutableList.of();

        TableInputFormat tableInputFormat = Mockito.mock(TableInputFormat.class);
        TableSplit tableSplit1 = Mockito.mock(TableSplit.class);
        TableSplit tableSplit2 = Mockito.mock(TableSplit.class);
        Mockito.when(tableSplit1.getTable()).thenReturn(TableName.valueOf(schema, table));
        Mockito.when(tableSplit1.getStartRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit1.getEndRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit1.getRegionLocation()).thenReturn("regionLocation1");
        Mockito.when(tableSplit1.getLength()).thenReturn(100L);
        Mockito.when(tableSplit2.getTable()).thenReturn(TableName.valueOf(schema, table));
        Mockito.when(tableSplit2.getStartRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit2.getEndRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit2.getRegionLocation()).thenReturn("regionLocation2");
        Mockito.when(tableSplit2.getLength()).thenReturn(200L);

        Mockito.when(connection.getConfiguration()).thenReturn(new JobConf());
        Mockito.when(connection.getTable(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getRegionLocator(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getAdmin()).thenReturn(null);

        Mockito.when(tableInputFormat.getSplits(ArgumentMatchers.any(JobContext.class)))
                .thenReturn(ImmutableList.of(tableSplit1, tableSplit2));

        List<TabletSplitMetadata> tabletSplits = hbaseClient.getTabletSplits(session, schema, table, rowIdDomain, constraints);

        assertEquals(2, tabletSplits.size());
        TabletSplitMetadata splitMetadata1 = tabletSplits.get(0);
        assertEquals(schema, splitMetadata1.getTableName());
        assertEquals(table, splitMetadata1.getTableName());
        TabletSplitMetadata splitMetadata2 = tabletSplits.get(1);
        assertEquals(schema, splitMetadata2.getTableName());
        assertEquals(table, splitMetadata2.getTableName());
    }

    @Test
    public void getTabletSplits_OptimizeLocalityEnabled_ReturnsOptimizedSplits() throws Exception {
        String schema = "testSchema";
        String table = "testTable";
        Optional<Domain> rowIdDomain = Optional.empty();
        List<HbaseColumnConstraint> constraints = ImmutableList.of();

        TableInputFormat tableInputFormat = Mockito.mock(TableInputFormat.class);
        TableSplit tableSplit = Mockito.mock(TableSplit.class);
        Mockito.when(tableSplit.getTable()).thenReturn(TableName.valueOf(schema, table));
        Mockito.when(tableSplit.getStartRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit.getEndRow()).thenReturn(new byte[]{});
        Mockito.when(tableSplit.getRegionLocation()).thenReturn("regionLocation");
        Mockito.when(tableSplit.getLength()).thenReturn(100L);

        Mockito.when(connection.getConfiguration()).thenReturn(new JobConf());
        Mockito.when(connection.getTable(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getRegionLocator(ArgumentMatchers.any(TableName.class))).thenReturn(null);
        Mockito.when(connection.getAdmin()).thenReturn(null);

        Mockito.when(tableInputFormat.getSplits(ArgumentMatchers.any(JobContext.class)))
                .thenReturn(ImmutableList.of(tableSplit));

        Mockito.when(session.getProperty("optimize_locality_enabled", Boolean.class))
                .thenReturn(true);

        List<TabletSplitMetadata> tabletSplits = hbaseClient.getTabletSplits(session, schema, table, rowIdDomain, constraints);

        assertEquals(1, tabletSplits.size());
        TabletSplitMetadata splitMetadata = tabletSplits.get(0);
        assertEquals(schema, splitMetadata.getTableName());
        assertEquals(table, splitMetadata.getTableName());
    }

    @Test
    public void getTabletSplits_ExceptionThrown_ThrowsPrestoException() throws Exception {
        String schema = "testSchema";
        String table = "testTable";
        Optional<Domain> rowIdDomain = Optional.empty();
        List<HbaseColumnConstraint> constraints = ImmutableList.of();

        Mockito.when(connection.getConfiguration()).thenThrow(new IOException("Test exception"));

        hbaseClient.getTabletSplits(session, schema, table, rowIdDomain, constraints);
    }
}
