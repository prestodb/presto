package com.facebook.presto.hbase.conf;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

/**
 * HBase catalog config
 * 
 * @author spancer.ray
 *
 */
public class HbaseConfig {
  private String hbaseMaster;
  private String zooKeepers;
  private String zkMetadataRoot = "/presto-hbase";

  private String zookeeperZnodeParent;

  private String krb5Conf;

  private String hbaseKeytabFile;

  private String hbaseKerberosPricipal;

  private String hbaseMasterKerberosPricipal;

  private String hbaseRegionserverPricipal;

  private boolean enableKerberos = true;
  private boolean dropToInternalTablesEnabled = false;// Can't drop internal HBase table by default.

  public String getHbaseMaster() {
    return hbaseMaster;
  }

  @Config("hbase.hosts")
  @ConfigDescription("IP:PORT where hbase master connect")
  public HbaseConfig setHbaseMaster(String hbaseMaster) {
    this.hbaseMaster = hbaseMaster;
    return this;
  }

  @NotNull
  public String getZooKeepers() {
    return this.zooKeepers;
  }

  @Config("hbase.zookeepers")
  @ConfigDescription("ZooKeeper quorum connect string for Hbase")
  public HbaseConfig setZooKeepers(String zooKeepers) {
    this.zooKeepers = zooKeepers;
    return this;
  }

  @NotNull
  public String getZkMetadataRoot() {
    return zkMetadataRoot;
  }

  @Config("hbase.zookeeper.metadata.root")
  @ConfigDescription("Sets the root znode for metadata storage")
  public void setZkMetadataRoot(String zkMetadataRoot) {
    this.zkMetadataRoot = zkMetadataRoot;
  }



    @NotNull
    public boolean isDropToInternalTablesEnabled() {
        return dropToInternalTablesEnabled;
    }
    
  @Config("hbase.internal.table.drop.enabled")
  @ConfigDescription("Enable drop to non-presto-managed (internal HBase) tables")
  public HbaseConfig setDropToInternalTablesEnabled(boolean dropToInternalTablesEnabled) {
    this.dropToInternalTablesEnabled = dropToInternalTablesEnabled;
    return this;
  }

    @NotNull
    public boolean getEnableKerberos() {
        return enableKerberos;
    }

    @Config("hbase.enable.kerberos")
    @ConfigDescription("Hbase enable kerberos")
    public HbaseConfig setEnableKerberos(boolean enableKerberos) {
        this.enableKerberos = enableKerberos;
        return this;
    }

    @NotNull
    public String getZookeeperZnodeParent() {
        return this.zookeeperZnodeParent;
    }
    @Config("zookeeper.znode.parent")
    @ConfigDescription("ZooKeeper znode parent connect string for Hbase")
    public HbaseConfig setZookeeperZnodeParent(String zookeeperZnodeParent) {
        this.zookeeperZnodeParent = zookeeperZnodeParent;
        return this;
    }

    @NotNull
    public String getKrb5Conf() {
        return this.krb5Conf;
    }
    @Config("java.security.krb5.conf")
    @ConfigDescription("krb5 connect string for Hbase")
    public HbaseConfig setKrb5Conf(String keb5Conf) {
        this.krb5Conf = keb5Conf;
        return this;
    }

    @NotNull
    public String getHbaseKeytabFile() {
        return this.hbaseKeytabFile;
    }
    @Config("hbase.keytab.file")
    @ConfigDescription("hbase keytab file connect string for Hbase")
    public HbaseConfig setHbaseKeytabFile(String hbaseKeytabFile) {
        this.hbaseKeytabFile = hbaseKeytabFile;
        return this;
    }

    @NotNull
    public String getHbaseKerberoPrincipal() {
        return this.hbaseKerberosPricipal;
    }
    @Config("hbase.kerberos.principal")
    @ConfigDescription("hbase kerberos principal connect string for Hbase")
    public HbaseConfig setHbaseKerberoPrincipal(String hbaseKerberosPricipal) {
        this.hbaseKerberosPricipal = hbaseKerberosPricipal;
        return this;
    }

    @NotNull
    public String getHbaseMasterKerberoPrincipal() {
        return this.hbaseMasterKerberosPricipal;
    }
    @Config("hbase.master.kerberos.principal")
    @ConfigDescription("hbase master kerberos principal connect string for Hbase")
    public HbaseConfig setHbaseMasterKerberoPrincipal(String hbaseMasterKerberosPricipal) {
        this.hbaseMasterKerberosPricipal = hbaseMasterKerberosPricipal;
        return this;
    }
    @NotNull
    public String getHbaseRegionserverKerberoPrincipal() {
        return this.hbaseRegionserverPricipal;
    }

    @Config("hbase.regionserver.kerberos.principal")
    @ConfigDescription("hbase regionserver kerberos principal connect string for Hbase")
    public HbaseConfig setHbaseRegionserverKerberoPrincipal(String hbaseRegionserverPricipal) {
        this.hbaseRegionserverPricipal = hbaseRegionserverPricipal;
        return this;
    }
}
