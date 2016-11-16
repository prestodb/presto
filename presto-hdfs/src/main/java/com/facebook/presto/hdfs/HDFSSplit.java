package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;

import java.util.List;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSSplit
implements ConnectorSplit {
    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return null;
    }

    @Override
    public Object getInfo() {
        return null;
    }
}
