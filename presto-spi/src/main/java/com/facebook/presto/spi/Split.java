package com.facebook.presto.spi;

import java.util.List;

public interface Split
{
    boolean isRemotelyAccessible();

    List<HostAddress> getAddresses();

    Object getInfo();

}
