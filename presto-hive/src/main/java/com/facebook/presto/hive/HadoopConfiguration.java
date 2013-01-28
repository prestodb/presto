package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.List;

public class HadoopConfiguration
{
    public static final ThreadLocal<Configuration> HADOOP_CONFIGURATION = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration config = new Configuration();

            // this is to prevent dfsclient from doing reverse DNS lookups to determine whether nodes are rack local
            config.setClass("topology.node.switch.mapping.impl", NoOpDNSwitchMapping.class, DNSToSwitchMapping.class);
            return config;
        }
    };

    public static class NoOpDNSwitchMapping
            implements DNSToSwitchMapping
    {
        @Override
        public List<String> resolve(List<String> names)
        {
            // dfsclient expects an empty list as an indication that the host->switch mapping for the given names are not known
            return ImmutableList.of();
        }
    }
}
