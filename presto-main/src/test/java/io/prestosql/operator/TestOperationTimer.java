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
package io.prestosql.operator;

import io.airlift.slice.XxHash64;
import io.prestosql.operator.OperationTimer.OperationTiming;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestOperationTimer
{
    public static volatile long blackHole;

    @Test
    public void testOverallTiming()
    {
        testOverallTiming(false);
        testOverallTiming(true);
    }

    private void testOverallTiming(boolean trackCpuTime)
    {
        InternalTiming timing = new InternalTiming(trackCpuTime);
        for (int i = 1; i <= 5; i++) {
            OperationTimer timer = new OperationTimer(trackCpuTime, false);
            doSomething();
            timing.record(timer::end);
        }
    }

    @Test
    public void testOperationTiming()
    {
        testOperationTiming(false);
        testOperationTiming(true);
    }

    private void testOperationTiming(boolean trackCpuTime)
    {
        InternalTiming overallTiming = new InternalTiming(true);

        InternalTiming operationTiming1 = new InternalTiming(trackCpuTime);
        InternalTiming operationTiming2 = new InternalTiming(trackCpuTime);
        InternalTiming operationTiming3 = new InternalTiming(trackCpuTime);

        OperationTimer timer = new OperationTimer(true, trackCpuTime);

        doSomething();
        operationTiming1.record(timer::recordOperationComplete);
        doSomething();
        operationTiming1.record(timer::recordOperationComplete);
        doSomething();
        operationTiming2.record(timer::recordOperationComplete);
        doSomething();
        operationTiming1.record(timer::recordOperationComplete);
        doSomething();
        operationTiming2.record(timer::recordOperationComplete);
        doSomething();
        operationTiming3.record(timer::recordOperationComplete);

        overallTiming.record(timer::end);

        assertThat(operationTiming1.getTiming().getWallNanos() + operationTiming2.getTiming().getWallNanos() + operationTiming3.getTiming().getWallNanos())
                .isLessThanOrEqualTo(overallTiming.getTiming().getWallNanos());
        assertThat(operationTiming1.getTiming().getCpuNanos() + operationTiming2.getTiming().getCpuNanos() + operationTiming3.getTiming().getCpuNanos())
                .isLessThanOrEqualTo(overallTiming.getTiming().getCpuNanos());
    }

    @Test
    public void testOperationAfterEndAreNotAllowed()
    {
        OperationTiming timing = new OperationTiming();
        OperationTimer timer = new OperationTimer(true, false);
        timer.end(timing);
        assertThatThrownBy(() -> timer.end(timing)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> timer.recordOperationComplete(timing)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testInvalidConstructorArguments()
    {
        assertThatThrownBy(() -> new OperationTimer(false, true)).isInstanceOf(IllegalArgumentException.class);
    }

    private static void doSomething()
    {
        byte[] data = new byte[10_000];
        new Random(blackHole).nextBytes(data);
        blackHole = XxHash64.hash(wrappedBuffer(data));
        sleepUninterruptibly(50, MILLISECONDS);
    }

    private static class InternalTiming
    {
        private final boolean trackCpuTime;

        private final OperationTiming timing = new OperationTiming();

        private long calls;
        private long previousWallNanos;
        private long previousCpuNanos;

        private InternalTiming(boolean trackCpuTime)
        {
            this.trackCpuTime = trackCpuTime;
        }

        public OperationTiming getTiming()
        {
            return timing;
        }

        public void record(Consumer<OperationTiming> timer)
        {
            previousWallNanos = timing.getWallNanos();
            previousCpuNanos = timing.getCpuNanos();
            assertEquals(timing.getCalls(), calls);
            timer.accept(timing);
            calls++;
            assertEquals(timing.getCalls(), calls);
            assertThat(timing.getWallNanos()).isGreaterThan(previousWallNanos);
            if (trackCpuTime) {
                assertThat(timing.getCpuNanos()).isGreaterThan(previousCpuNanos);
                assertThat(timing.getWallNanos()).isGreaterThan(timing.getCpuNanos());
            }
            else {
                assertEquals(timing.getCpuNanos(), 0);
            }
        }
    }
}
