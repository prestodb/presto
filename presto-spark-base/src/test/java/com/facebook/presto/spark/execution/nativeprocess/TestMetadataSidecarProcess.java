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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spi.PrestoException;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMetadataSidecarProcess
{
    private static final String CRASH_BANNER =
            "*** Aborted at 1784277903 (Unix time) ***\n" +
            "*** Signal 11 (SIGSEGV) (0x0) received by PID 3640477 ***\n" +
            "    @ facebook::velox::exec::Driver::runInternal";

    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testStartFailureCarriesCrashReport()
    {
        PrestoException thrown = startFailureOf(CRASH_BANNER);
        String message = thrown.getMessage();

        assertEquals(thrown.getErrorCode(), NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR.toErrorCode());
        assertTrue(message.startsWith("Failed to start metadata sidecar"), message);
        assertTrue(message.contains("*** Signal 11 (SIGSEGV)"), "missing signal line: " + message);
        assertTrue(message.contains("Driver::runInternal"), "missing crashing frame: " + message);
    }

    /**
     * A sidecar that produced no banner — a bad binary path, or a kill that prints nothing —
     * must not leave a dangling separator on the message.
     */
    @Test
    public void testStartFailureWithoutCrashReport()
    {
        String message = startFailureOf("").getMessage();

        assertTrue(message.startsWith("Failed to start metadata sidecar"), message);
        assertFalse(message.contains(":\n"), "dangling separator with no banner: " + message);
    }

    /**
     * The port-discovery death path reports through this helper rather than through
     * propagateStartFailure, so the banner has to reach the message from here too.
     */
    @Test
    public void testWithCrashReportEnrichesAnyMessage()
    {
        assertEquals(
                processWithCrashReport(CRASH_BANNER).withCrashReport("exited before writing port file"),
                "exited before writing port file:\n" + CRASH_BANNER);
        assertEquals(
                processWithCrashReport("").withCrashReport("exited before writing port file"),
                "exited before writing port file");
    }

    private PrestoException startFailureOf(String crashReport)
    {
        try {
            throw processWithCrashReport(crashReport).propagateStartFailure(new RuntimeException("startup timed out"));
        }
        catch (PrestoException e) {
            return e;
        }
    }

    private MetadataSidecarProcess processWithCrashReport(String crashReport)
    {
        return new MetadataSidecarProcess(
                "/nonexistent/presto_server",
                "",
                new OkHttpClient(),
                Runnable::run,
                scheduledExecutor,
                jsonCodec(ServerInfo.class),
                new Duration(1, SECONDS),
                "oncall",
                "user",
                "service")
        {
            @Override
            public String getCrashReport()
            {
                return crashReport;
            }
        };
    }
}
