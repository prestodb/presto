/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.cli;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;

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

    public boolean isNullPager()
    {
        return process == null;
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

    public CompletableFuture<?> getFinishFuture()
    {
        checkState(process != null, "getFinishFuture called on null pager");
        CompletableFuture<?> result = new CompletableFuture<>();
        new Thread(() -> {
            try {
                process.waitFor();
            }
            catch (InterruptedException e) {
                // ignore exception because thread is exiting
            }
            finally {
                result.complete(null);
            }
        }).start();
        return unmodifiableFuture(result);
    }

    private static IOException propagateIOException(IOException e)
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
