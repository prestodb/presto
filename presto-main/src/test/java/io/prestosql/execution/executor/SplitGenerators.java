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
package io.prestosql.execution.executor;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.prestosql.execution.executor.SplitSpecification.IntermediateSplitSpecification;
import io.prestosql.execution.executor.SplitSpecification.LeafSplitSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.execution.executor.Histogram.fromContinuous;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

class SplitGenerators
{
    private SplitGenerators() {}

    public static void main(String[] args)
    {
        Histogram<Long> bins = fromContinuous(ImmutableList.of(
                MILLISECONDS.toNanos(0),
                MILLISECONDS.toNanos(1),
                MILLISECONDS.toNanos(10),
                MILLISECONDS.toNanos(100),
                MILLISECONDS.toNanos(1_000),
                MILLISECONDS.toNanos(10_000),
                MILLISECONDS.toNanos(60_000),
                MILLISECONDS.toNanos(300_000),
                MINUTES.toNanos(20),
                DAYS.toNanos(1)));

        IntermediateSplitGenerator intermediateSplitGenerator = new IntermediateSplitGenerator(null);
        List<IntermediateSplitSpecification> intermediateSpecs = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            IntermediateSplitSpecification next = intermediateSplitGenerator.next();
            intermediateSpecs.add(next);
        }

        System.out.println("Scheduled time distributions");
        System.out.println("============================");
        System.out.println();
        System.out.println("Tasks with 8x " + IntermediateSplitGenerator.class.getSimpleName());
        bins.printDistribution(intermediateSpecs, t -> t.getScheduledTimeNanos() * 8, a -> 1, Duration::succinctNanos, a -> "");

        List<SplitGenerator> leafSplitGenerators = ImmutableList.of(
                new FastLeafSplitGenerator(),
                new SlowLeafSplitGenerator(),
                new L4LeafSplitGenerator(),
                new QuantaExceedingSplitGenerator(),
                new AggregatedLeafSplitGenerator());

        for (SplitGenerator generator : leafSplitGenerators) {
            List<SplitSpecification> leafSpecs = new ArrayList<>();
            for (int i = 0; i < 17000; i++) {
                leafSpecs.add(generator.next());
            }

            System.out.println();
            System.out.println("Tasks with 4x " + generator.getClass().getSimpleName());
            bins.printDistribution(leafSpecs, t -> t.getScheduledTimeNanos() * 4, Duration::succinctNanos);

            System.out.println("Per quanta:");
            bins.printDistribution(leafSpecs, SplitSpecification::getPerQuantaNanos, Duration::succinctNanos);
        }
    }

    interface SplitGenerator
    {
        SplitSpecification next();
    }

    public static class IntermediateSplitGenerator
            implements SplitGenerator
    {
        private final ScheduledExecutorService wakeupExecutor;

        IntermediateSplitGenerator(ScheduledExecutorService wakeupExecutor)
        {
            this.wakeupExecutor = wakeupExecutor;
        }

        public IntermediateSplitSpecification next()
        {
            long numQuanta = generateIntermediateSplitNumQuanta(0, 1);

            long wallNanos = MILLISECONDS.toNanos(generateIntermediateSplitWallTimeMs(0, 1));
            long scheduledNanos = MILLISECONDS.toNanos(generateIntermediateSplitScheduledTimeMs(0, 1));

            long blockedNanos = (long) (ThreadLocalRandom.current().nextDouble(0.97, 0.99) * wallNanos);

            long perQuantaNanos = scheduledNanos / numQuanta;
            long betweenQuantaNanos = blockedNanos / numQuanta;

            return new IntermediateSplitSpecification(scheduledNanos, wallNanos, numQuanta, perQuantaNanos, betweenQuantaNanos, wakeupExecutor);
        }
    }

    public static class AggregatedLeafSplitGenerator
            implements SplitGenerator
    {
        public LeafSplitSpecification next()
        {
            long totalNanos = MILLISECONDS.toNanos(generateLeafSplitScheduledTimeMs(0, 1));
            long quantaNanos = Math.min(totalNanos, MICROSECONDS.toNanos(generateLeafSplitPerCallMicros(0, 1)));

            return new LeafSplitSpecification(totalNanos, quantaNanos);
        }
    }

    public static class FastLeafSplitGenerator
            implements SplitGenerator
    {
        public LeafSplitSpecification next()
        {
            long totalNanos = MILLISECONDS.toNanos(generateLeafSplitScheduledTimeMs(0, 0.75));
            long quantaNanos = Math.min(totalNanos, MICROSECONDS.toNanos(generateLeafSplitPerCallMicros(0, 1)));

            return new LeafSplitSpecification(totalNanos, quantaNanos);
        }
    }

    public static class SlowLeafSplitGenerator
            implements SplitGenerator
    {
        public LeafSplitSpecification next()
        {
            long totalNanos = MILLISECONDS.toNanos(generateLeafSplitScheduledTimeMs(0.75, 1));
            long quantaNanos = Math.min(totalNanos, MICROSECONDS.toNanos(generateLeafSplitPerCallMicros(0, 1)));

            return new LeafSplitSpecification(totalNanos, quantaNanos);
        }
    }

    public static class L4LeafSplitGenerator
            implements SplitGenerator
    {
        public LeafSplitSpecification next()
        {
            long totalNanos = MILLISECONDS.toNanos(generateLeafSplitScheduledTimeMs(0.99, 1));
            long quantaNanos = Math.min(totalNanos, MICROSECONDS.toNanos(generateLeafSplitPerCallMicros(0, 0.9)));

            return new LeafSplitSpecification(totalNanos, quantaNanos);
        }
    }

    public static class QuantaExceedingSplitGenerator
            implements SplitGenerator
    {
        public LeafSplitSpecification next()
        {
            long totalNanos = MILLISECONDS.toNanos(generateLeafSplitScheduledTimeMs(0.99, 1));
            long quantaNanos = Math.min(totalNanos, MICROSECONDS.toNanos(generateLeafSplitPerCallMicros(0.75, 1)));

            return new LeafSplitSpecification(totalNanos, quantaNanos);
        }
    }

    public static class SimpleLeafSplitGenerator
            implements SplitGenerator
    {
        private final long totalNanos;
        private final long quantaNanos;

        public SimpleLeafSplitGenerator(long totalNanos, long quantaNanos)
        {
            this.totalNanos = totalNanos;
            this.quantaNanos = quantaNanos;
        }

        public LeafSplitSpecification next()
        {
            return new LeafSplitSpecification(totalNanos, quantaNanos);
        }
    }

    // these numbers come from real world stats
    private static long generateLeafSplitScheduledTimeMs(double origin, double bound)
    {
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        double value = generator.nextDouble(origin, bound);
        // in reality, max is several hours, but this would make the simulation too slow
        if (value > 0.998) {
            return generator.nextLong(5 * 60 * 1000, 10 * 60 * 1000);
        }

        if (value > 0.99) {
            return generator.nextLong(60 * 1000, 5 * 60 * 1000);
        }

        if (value > 0.95) {
            return generator.nextLong(10_000, 60 * 1000);
        }

        if (value > 0.50) {
            return generator.nextLong(1000, 10_000);
        }

        if (value > 0.25) {
            return generator.nextLong(100, 1000);
        }

        if (value > 0.10) {
            return generator.nextLong(10, 100);
        }

        return generator.nextLong(1, 10);
    }

    private static long generateLeafSplitPerCallMicros(double origin, double bound)
    {
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        double value = generator.nextDouble(origin, bound);
        if (value > 0.9999) {
            return 200_000_000;
        }

        if (value > 0.99) {
            return generator.nextLong(3_000_000, 15_000_000);
        }

        if (value > 0.95) {
            return generator.nextLong(2_000_000, 5_000_000);
        }

        if (value > 0.90) {
            return generator.nextLong(1_500_000, 5_000_000);
        }

        if (value > 0.75) {
            return generator.nextLong(1_000_000, 2_000_000);
        }

        if (value > 0.50) {
            return generator.nextLong(500_000, 1_000_000);
        }

        if (value > 0.1) {
            return generator.nextLong(100_000, 500_000);
        }

        return generator.nextLong(250, 500);
    }

    private static long generateIntermediateSplitScheduledTimeMs(double origin, double bound)
    {
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        double value = generator.nextDouble(origin, bound);
        // in reality, max is several hours, but this would make the simulation too slow

        if (value > 0.999) {
            return generator.nextLong(5 * 60 * 1000, 10 * 60 * 1000);
        }

        if (value > 0.99) {
            return generator.nextLong(60 * 1000, 5 * 60 * 1000);
        }

        if (value > 0.95) {
            return generator.nextLong(10_000, 60 * 1000);
        }

        if (value > 0.75) {
            return generator.nextLong(1000, 10_000);
        }

        if (value > 0.45) {
            return generator.nextLong(100, 1000);
        }

        if (value > 0.20) {
            return generator.nextLong(10, 100);
        }

        return generator.nextLong(1, 10);
    }

    private static long generateIntermediateSplitWallTimeMs(double origin, double bound)
    {
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        double value = generator.nextDouble(origin, bound);
        // in reality, max is several hours, but this would make the simulation too slow

        if (value > 0.90) {
            return generator.nextLong(400_000, 800_000);
        }

        if (value > 0.75) {
            return generator.nextLong(100_000, 200_000);
        }

        if (value > 0.50) {
            return generator.nextLong(50_000, 100_000);
        }

        if (value > 0.40) {
            return generator.nextLong(30_000, 50_000);
        }

        if (value > 0.30) {
            return generator.nextLong(20_000, 30_000);
        }

        if (value > 0.20) {
            return generator.nextLong(10_000, 15_000);
        }

        if (value > 0.10) {
            return generator.nextLong(5_000, 10_000);
        }

        return generator.nextLong(1_000, 5_000);
    }

    private static long generateIntermediateSplitNumQuanta(double origin, double bound)
    {
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        double value = generator.nextDouble(origin, bound);

        if (value > 0.95) {
            return generator.nextLong(2000, 20_000);
        }

        if (value > 0.90) {
            return generator.nextLong(1_000, 2_000);
        }

        return generator.nextLong(10, 1000);
    }
}
