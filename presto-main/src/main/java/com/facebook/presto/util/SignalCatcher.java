package com.facebook.presto.util;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;

public class SignalCatcher
        implements Closeable
{
    public static final Signal SIGINT = new Signal("INT");

    private final SignalHandler oldHandler;
    private final Signal signal;

    public SignalCatcher(Signal signal)
    {
        this(signal, new SignalHandler()
        {
            @Override
            public void handle(Signal ignored)
            {
                // ignore
            }
        });
    }

    public SignalCatcher(Signal signal, SignalHandler handler)
    {
        this.signal = signal;
        this.oldHandler = Signal.handle(signal, handler);
    }

    @Override
    public void close()
    {
        Signal.handle(signal, oldHandler);
    }
}
