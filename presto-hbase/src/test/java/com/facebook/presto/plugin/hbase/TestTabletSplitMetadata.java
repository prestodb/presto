package com.facebook.presto.plugin.hbase;


import com.facebook.presto.hbase.TabletSplitMetadata;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestTabletSplitMetadata {

    private Scan scan;

    @BeforeMethod
    public void setUp() {
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes("startRow"));
        scan.setStopRow(Bytes.toBytes("stopRow"));
        scan.addColumn(Bytes.toBytes("family"), Bytes.toBytes("qualifier"));
    }

    @Test
    public void convertScanToString_ShouldReturnCorrectString() throws IOException {
        String scanString = TabletSplitMetadata.convertScanToString(scan);
        assertNotNull(scanString, "The scan string should not be null");
        assertEquals(scanString, "ROWPREFIXFILTER \\x73\\x74\\x61\\x72\\x74\\x52\\x6F\\x77\\x2C\\x73\\x74\\x6F\\x70\\x52\\x6F\\x77\\x2C\\x66\\x61\\x6D\\x69\\x6C\\x79\\x3A\\x71\\x75\\x61\\x6C\\x69\\x66\\x69\\x65\\x72\\x2C\\x66\\x61\\x6D\\x69\\x6C\\x79\\x3A\\x71\\x75\\x61\\x6C\\x69\\x66\\x69\\x65\\x72");
    }

    @Test
    public void convertStringToScan_ShouldReturnCorrectScan() throws IOException {
        String scanString = "ROWPREFIXFILTER \\x73\\x74\\x61\\x72\\x74\\x52\\x6F\\x77\\x2C\\x73\\x74\\x6F\\x70\\x52\\x6F\\x77\\x2C\\x66\\x61\\x6D\\x69\\x6C\\x79\\x3A\\x71\\x75\\x61\\x6C\\x69\\x66\\x69\\x65\\x72\\x2C\\x66\\x61\\x6D\\x69\\x6C\\x79\\x3A\\x71\\x75\\x61\\x6C\\x69\\x66\\x69\\x65\\x72";
        Scan convertedScan = TabletSplitMetadata.convertStringToScan(scanString);
        assertNotNull(convertedScan, "The converted scan should not be null");
        assertEquals(convertedScan.getStartRow(), Bytes.toBytes("startRow"));
        assertEquals(convertedScan.getStopRow(), Bytes.toBytes("stopRow"));
        assertEquals(convertedScan.getFamilyMap().size(), 1);
        assertEquals(convertedScan.getFamilyMap().get(Bytes.toBytes("family")).size(), 1);
    }

    @Test
    public void convertStringToScan_ShouldHandleOutputFromConvertScanToString() throws IOException {
        String scanString = TabletSplitMetadata.convertScanToString(scan);
        Scan convertedScan = TabletSplitMetadata.convertStringToScan(scanString);
        assertNotNull(convertedScan, "The converted scan should not be null");
        assertEquals(convertedScan.getStartRow(), scan.getStartRow());
        assertEquals(convertedScan.getStopRow(), scan.getStopRow());
        assertEquals(convertedScan.getFamilyMap().size(), scan.getFamilyMap().size());
        assertEquals(convertedScan.getFamilyMap().get(Bytes.toBytes("family")).size(), scan.getFamilyMap().get(Bytes.toBytes("family")).size());
    }
}
