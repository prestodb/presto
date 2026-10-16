package com.facebook.presto.hbase;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * 
 * @author spancer.ray
 *
 */
public class HbasePlugin implements Plugin {
  @Override
  public Set<Class<?>> getFunctions() {
    return ImmutableSet.of();
  }

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new HbaseConnectorFactory("hbase"));
  }
}
