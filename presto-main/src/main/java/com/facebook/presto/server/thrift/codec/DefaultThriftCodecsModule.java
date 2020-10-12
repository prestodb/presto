package com.facebook.presto.server.thrift.codec;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;

public class DefaultThriftCodecsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(UriThriftCodec.class);
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(UuidThriftCodec.class);
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(LocaleCodec.class);
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(DurationToMillisThriftCodec.class);
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(DataSizeToBytesThriftCodec.class);
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(DateTimeToEpochMillisThriftCodec.class);
        thriftCodecBinder(binder)
                .bindCustomThriftCodec(ExecutionFailureInfoCodec.class);
    }
}
