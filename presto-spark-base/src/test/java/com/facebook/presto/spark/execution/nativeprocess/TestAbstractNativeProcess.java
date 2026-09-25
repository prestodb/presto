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
package com.facebook.presto.spark.execution.nativeprocess;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAbstractNativeProcess
{
    /** Capture starts at the "*** Aborted" line; earlier log lines are teed to stderr but left out. */
    @Test
    public void testCrashBannerCaptured()
    {
        String stderr = "I0715 12:00:00 native log line 1\n" +
                "I0715 12:00:01 native log line 2\n" +
                "*** Aborted at 1784160518 (Unix time, try 'date -d @1784160518') ***\n" +
                "*** Signal 11 (SIGSEGV) (0x110) received by PID 123 (code: address not mapped to object) ***\n" +
                "    @ facebook::velox::exec::TopNRowNumber::reclaim\n" +
                "Fatal signal handler. Query Id= 20260716_000432_00000_34rky Task Id= 20260716_000432_00000_34rky.9.0.98.0\n";
        ByteArrayInputStream in = new ByteArrayInputStream(stderr.getBytes(UTF_8));
        ByteArrayOutputStream tee = new ByteArrayOutputStream();

        AbstractNativeProcess.ProcessOutputPipe pipe = new AbstractNativeProcess.ProcessOutputPipe(123, in, tee);
        // Run synchronously: the reader loop fills abortMessage and returns at stderr EOF.
        pipe.run();

        String crashReport = pipe.getAbortMessage();
        assertTrue(crashReport.contains("*** Aborted at 1784160518"), "missing abort banner: " + crashReport);
        assertTrue(crashReport.contains("*** Signal 11 (SIGSEGV)"), "missing signal line: " + crashReport);
        assertTrue(crashReport.contains("TopNRowNumber::reclaim"), "missing native frame: " + crashReport);
        assertTrue(crashReport.contains("Query Id= 20260716_000432_00000_34rky"), "missing query id: " + crashReport);
        // Capture starts at the banner: earlier ordinary log lines are not part of the crash report.
        assertFalse(crashReport.contains("native log line 1"), "should not capture pre-abort lines: " + crashReport);

        // Every line (including pre-crash logs) is still teed to the executor's stderr.
        String teed = new String(tee.toByteArray(), UTF_8);
        assertTrue(teed.contains("native log line 1"), "tee is missing a normal log line");
        assertTrue(teed.contains("*** Aborted at 1784160518"), "tee is missing the crash banner");
    }

    /** A death with no banner (SIGKILL / OOM-kill) drains cleanly and yields an empty report. */
    @Test
    public void testNoBannerYieldsEmptyCrashReport()
    {
        String stderr = "I0715 12:00:00 line a\nI0715 12:00:01 line b\n";
        ByteArrayInputStream in = new ByteArrayInputStream(stderr.getBytes(UTF_8));
        ByteArrayOutputStream tee = new ByteArrayOutputStream();

        AbstractNativeProcess.ProcessOutputPipe pipe = new AbstractNativeProcess.ProcessOutputPipe(1, in, tee);
        pipe.run();

        assertTrue(pipe.getAbortMessage().isEmpty(), "expected empty crash report when no banner is present");
        assertTrue(new String(tee.toByteArray(), UTF_8).contains("line a"), "tee is missing a normal log line");
    }

    /**
     * fd 2 is shared by every worker's pipe on the executor, so a pipe that closed it would break the
     * next relaunched worker's pipe before that pipe reaches its banner.
     */
    @Test
    public void testPipeDoesNotCloseSharedStderr()
    {
        // Models fd 2: once closed, further writes throw.
        class SharedStderr
                extends OutputStream
        {
            private final ByteArrayOutputStream sink = new ByteArrayOutputStream();
            private volatile boolean closed;

            @Override
            public void write(int b)
                    throws IOException
            {
                if (closed) {
                    throw new IOException("Stream Closed");
                }
                sink.write(b);
            }

            @Override
            public void close()
            {
                closed = true;
            }
        }
        SharedStderr stderr = new SharedStderr();

        // First worker crash: banner is captured AND the shared stderr must stay open.
        String crash1 = "I0715 12:00:00 starting\n*** Aborted at 1 ***\n*** Signal 11 (SIGSEGV) ***\n";
        AbstractNativeProcess.ProcessOutputPipe pipe1 = new AbstractNativeProcess.ProcessOutputPipe(
                1, new ByteArrayInputStream(crash1.getBytes(UTF_8)), stderr);
        pipe1.run();
        assertTrue(pipe1.getAbortMessage().contains("*** Aborted at 1"), "pipe1 missed its banner");
        assertFalse(stderr.closed, "pipe must not close the shared executor stderr (FileDescriptor.err)");

        // Relaunched worker on the SAME executor: its banner must still be captured.
        String crash2 = "I0715 12:02:00 starting\n*** Aborted at 2 ***\n*** Signal 11 (SIGSEGV) ***\n";
        AbstractNativeProcess.ProcessOutputPipe pipe2 = new AbstractNativeProcess.ProcessOutputPipe(
                2, new ByteArrayInputStream(crash2.getBytes(UTF_8)), stderr);
        pipe2.run();
        assertTrue(pipe2.getAbortMessage().contains("*** Aborted at 2"), "pipe2 missed its banner (shared stderr was closed)");

        // The shared stderr stayed writable across both crashes: both workers' output was teed to it.
        String teed = new String(stderr.sink.toByteArray(), UTF_8);
        assertTrue(teed.contains("*** Aborted at 1"), "shared stderr missing first worker's output: " + teed);
        assertTrue(teed.contains("*** Aborted at 2"), "shared stderr missing second worker's output: " + teed);
    }

    /**
     * The pipe runs on an unjoined daemon thread, so a caller that has just observed the
     * process die can reach the banner while it is still being appended.
     */
    @Test
    public void testAwaitDrainedWaitsForTheWholeBanner()
    {
        String crash = "*** Aborted at 1 ***\n*** Signal 6 (SIGABRT) ***\n    @ theLastFrame\n";
        // Yields one byte per read so the reader is still mid-banner when awaitDrained is called.
        InputStream trickle = new ByteArrayInputStream(crash.getBytes(UTF_8))
        {
            @Override
            public synchronized int read(byte[] buffer, int offset, int length)
            {
                return super.read(buffer, offset, 1);
            }
        };

        AbstractNativeProcess.ProcessOutputPipe pipe =
                new AbstractNativeProcess.ProcessOutputPipe(7, trickle, new ByteArrayOutputStream());
        pipe.start();

        assertTrue(pipe.awaitDrained(30, TimeUnit.SECONDS), "drain reported incomplete on a stream that reached EOF");
        String crashReport = pipe.getAbortMessage();
        assertTrue(
                crashReport.contains("theLastFrame"),
                "awaitDrained returned before the banner was complete: " + crashReport);
    }

    /**
     * A drain that never finishes has to say so, otherwise the caller reports whatever partial
     * banner it has as though it were the whole crash.
     */
    @Test
    public void testAwaitDrainedReportsAnUnfinishedPipe()
    {
        // Never started, so the reader never reaches EOF and the latch never opens.
        AbstractNativeProcess.ProcessOutputPipe pipe = new AbstractNativeProcess.ProcessOutputPipe(
                8, new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream());

        assertFalse(
                pipe.awaitDrained(50, TimeUnit.MILLISECONDS),
                "a pipe that never ran must not report a completed drain");
    }
}
