package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class Pager
        extends FilterOutputStream
{
    public static final String ENV_PAGER = "PRESTO_PAGER";
    public static final List<String> LESS = ImmutableList.of("less", "-FXRSn");

    private final Process process;

    private Pager(OutputStream out, @Nullable Process process)
    {
        super(out);
        this.process = process;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            super.close();
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
        finally {
            if (process != null) {
                try {
                    process.waitFor();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    process.destroy();
                }
            }
        }
    }

    @Override
    public void write(int b)
            throws IOException
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
            throws IOException
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
            throws IOException
    {
        // TODO: check if the pager exited and verify the exit status?
        if ("Broken pipe".equals(e.getMessage()) || "Stream closed".equals(e.getMessage())) {
            throw new QueryAbortedException(e);
        }
        throw e;
    }

    public static Pager create()
    {
        String pager = System.getenv(ENV_PAGER);
        if (pager == null) {
            return create(LESS);
        }
        pager = pager.trim();
        if (pager.isEmpty()) {
            return createNullPager();
        }
        return create(ImmutableList.of("/bin/sh", "-c", pager));
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
            System.err.println("ERROR: failed to open pager: " + e.getMessage());
            return createNullPager();
        }
    }

    private static Pager createNullPager()
    {
        return new Pager(uncloseableOutputStream(System.out), null);
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
