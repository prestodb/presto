package com.facebook.presto.plugin.hbase;


import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hbase.HbaseClient;
import com.facebook.presto.hbase.HbaseConnectorId;
import com.facebook.presto.hbase.HbaseSplitManager;
import com.facebook.presto.hbase.TabletSplitMetadata;

import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.hbase.model.HbaseTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.testng.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHbaseSplitManager {

    private HbaseSplitManager splitManager;
    private HbaseClient mockClient;
    private ConnectorSession mockSession;
    private HbaseTableLayoutHandle mockLayout;

    @BeforeMethod
    public void setUp() {
        mockClient = mock(HbaseClient.class);
        mockSession = mock(ConnectorSession.class);
        mockLayout = mock(HbaseTableLayoutHandle.class);
        splitManager = new HbaseSplitManager(new HbaseConnectorId("test"), mockClient);
    }

    @Test
    public void getSplits_NoConstraints_EmptySplits() {
        HbaseTableHandle tableHandle = new HbaseTableHandle("test", "schema", "table", "rowId", Optional.empty());
        HbaseTableLayoutHandle layoutHandle = new HbaseTableLayoutHandle(tableHandle, TupleDomain.all());

        when(mockLayout.getTable()).thenReturn(tableHandle);
        when(mockLayout.getConstraint()).thenReturn(TupleDomain.all());

        ConnectorSplitSource splitSource = splitManager.getSplits(null, mockSession, mockLayout, null);

        assertTrue(splitSource.isFinished());
        assertEquals(splitSource.getNextBatch(null, 1000).join().getSplits().size(), 0);
    }

    @Test
    public void getSplits_WithConstraints_NonEmptySplits() throws Exception {
        HbaseTableHandle tableHandle = new HbaseTableHandle("test", "schema", "table", "rowId", Optional.empty());
        HbaseTableLayoutHandle layoutHandle = new HbaseTableLayoutHandle(tableHandle, TupleDomain.all());

        when(mockLayout.getTable()).thenReturn(tableHandle);
        when(mockLayout.getConstraint()).thenReturn(TupleDomain.all());

        TabletSplitMetadata splitMetadata = new TabletSplitMetadata("tableName".getBytes(), "startRow".getBytes(), "endRow".getBytes(), "scan", "regionLocation", 100L);
        when(mockClient.getTabletSplits(any(ConnectorSession.class), eq("schema"), eq("table"), any(Optional.class), anyList()))
                .thenReturn(ImmutableList.of(splitMetadata));

        ConnectorSplitSource splitSource = splitManager.getSplits(null, mockSession, mockLayout, null);

        assertTrue(splitSource.isFinished());
        List<ConnectorSplit> splits = splitSource.getNextBatch(null, 1000).join().getSplits();
        assertEquals(splits.size(), 1);
        assertTrue(splits.get(0) instanceof HbaseSplit);
    }
}
