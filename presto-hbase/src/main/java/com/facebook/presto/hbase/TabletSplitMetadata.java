package com.facebook.presto.hbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TabletSplitMetadata {
  private byte[] tableName;
  private final byte[] startRow;
  private final byte[] endRow;
  private final String regionLocation;
  private final String scan; // stores the serialized form of the Scan
  private final long length; // Contains estimation of region size in bytes

  @JsonCreator
  public TabletSplitMetadata(@JsonProperty("tableName") byte[] tableName,
      @JsonProperty("startRow") byte[] startRow, @JsonProperty("endRow") byte[] endRow,
      @JsonProperty("scan") String scan, @JsonProperty("regionLocation") String regionLocation,
      @JsonProperty("length") long length) {
    this.tableName = requireNonNull(tableName, "hostPort is null");
    this.startRow = requireNonNull(startRow, "hostPort is null");
    this.endRow = requireNonNull(endRow, "endRow is null");
    this.regionLocation = requireNonNull(regionLocation, "regionLocation is null");
    this.scan = requireNonNull(scan, "scan is null");
    this.length = length;
  }

  public static String convertScanToString(Scan scan) throws IOException {
    String scanString = TableMapReduceUtil.convertScanToString(scan);
    return scanString;
  }

  public static Scan convertStringToScan(String scan) throws IOException {
    return TableMapReduceUtil.convertStringToScan(scan);
  }

  @JsonProperty
  public byte[] getTableName() {
    return tableName;
  }

  @JsonProperty
  public byte[] getStartRow() {
    return startRow;
  }

  @JsonProperty
  public byte[] getEndRow() {
    return endRow;
  }

  @JsonProperty
  public String getRegionLocation() {
    return regionLocation;
  }

  @JsonProperty
  public String getScan() {
    return scan;
  }

  @JsonProperty
  public long getLength() {
    return length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, startRow, endRow, regionLocation, scan, length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    TabletSplitMetadata other = (TabletSplitMetadata) obj;
    return Arrays.equals(this.tableName, other.tableName)
        && Arrays.equals(this.startRow, other.startRow) && Arrays.equals(this.endRow, other.endRow)
        && Objects.equals(this.regionLocation, other.regionLocation)
        && Objects.equals(this.scan, other.scan) && Objects.equals(this.length, other.length);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("tableName", tableName).add("startRow", startRow)
        .add("endRow", endRow).add("regionLocation", regionLocation).add("scan", scan)
        .add("length", length).toString();
  }
}
