package com.facebook.presto.example.prac;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

/**
 * @Author LTR
 * @Date 2025/4/15 16:46
 * @注释
 */
public class ExamplePracPlugin implements Plugin {
    /**
     *
     * @return
     */
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories(){
        return ImmutableList.of(new ExamplePracFactory());
    }
}

