package com.facebook.presto.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Pager
        extends FilterOutputStream
{
    public static final List<String> LESS = ImmutableList.of("less", "-FXRSn");

    private final Process process;

    private Pager(OutputStream out, @Nullable Process process)
    {
        super(out);
        this.process = process;
    }

    @Override
    public void close()
    {
        try {
            super.close();
        }
        catch (IOException ignored) {
        }
        finally {
            if (process != null) {
                try {
                    process.waitFor();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void write(int b)
    {
        try {
            super.write(b);
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        try {
            super.write(b, off, len);
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        try {
            super.flush();
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
    }

    private static RuntimeException propagateIOException(IOException e)
    {
        // TODO: check if the pager exited and verify the exit status?
        if ("Broken pipe".equals(e.getMessage())) {
            throw new QueryAbortedException(e);
        }
        throw Throwables.propagate(e);
    }

    public static Pager create(List<String> command)
    {
        try {
            Process process = new ProcessBuilder()
                    .command(command)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            return new Pager(process.getOutputStream(), process);
        }
        catch (IOException e) {
            // TODO: make this a supplier and only print the error once
            System.err.println("ERROR: failed to open pager: " + e.getMessage());
            return new Pager(uncloseableOutputStream(System.out), null);
        }
    }

    private static OutputStream uncloseableOutputStream(OutputStream out)
    {
        return new FilterOutputStream(out)
        {
            @Override
            public void close()
                    throws IOException
            {
                flush();
            }
        };
    }
}
