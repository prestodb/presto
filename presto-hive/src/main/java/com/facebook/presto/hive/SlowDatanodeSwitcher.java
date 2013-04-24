package com.facebook.presto.hive;

import com.facebook.presto.hive.util.Distribution2;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;

public class SlowDatanodeSwitcher
{
    private static final Logger log = Logger.get(SlowDatanodeSwitcher.class);

    private static final Method setUnfavoredNodesMethod;
    static {
        Method method;
        try {
            method = DFSDataInputStream.class.getMethod("setUnfavoredNodes", Collection.class);
        }
        catch (NoSuchMethodException e) {
            method = null;
        }
        setUnfavoredNodesMethod = method;
    }

    public static boolean isSupported()
    {
        return setUnfavoredNodesMethod != null;
    }

    private final Cache<DatanodeInfo, DatanodeInfo> unfavoredNodes;
    private final Distribution2 globalDistribution;
    private final double streamRateDecayAlpha;
    private final DataSize minMonitorThreshold;
    private final Duration minStreamSamplingTime;
    private final int minGlobalSamples;
    private final DataSize minStreamRate;
    private final int slowStreamPercentile;

    @Inject
    public SlowDatanodeSwitcher(HiveClientConfig config)
    {
        Preconditions.checkNotNull(config, "config is null");
        streamRateDecayAlpha = ExponentialDecay.seconds((int) config.getStreamRateDecay().convertTo(SECONDS));
        minMonitorThreshold = config.getMinMonitorThreshold();
        minStreamSamplingTime = config.getMinStreamSamplingTime();
        minGlobalSamples = config.getMinGlobalSamples();
        minStreamRate = config.getMinStreamRate();
        slowStreamPercentile = config.getSlowStreamPercentile();

        Duration unfavoredNodeCacheTime = config.getUnfavoredNodeCacheTime();
        unfavoredNodes = CacheBuilder.newBuilder()
                .expireAfterWrite((long) unfavoredNodeCacheTime.toMillis(), TimeUnit.MILLISECONDS)
                .build();
        Duration globalDistributionDecay = config.getGlobalDistributionDecay();
        globalDistribution = new Distribution2(ExponentialDecay.seconds((int) globalDistributionDecay.convertTo(SECONDS)));
    }

    public Function<FileSystem, FileSystem> createFileSystemWrapper()
    {
        return new Function<FileSystem, FileSystem>()
        {
            @Override
            public FileSystem apply(FileSystem fileSystem)
            {
                return new ForwardingFileSystem(fileSystem)
                {
                    @Override
                    public FSDataInputStream open(Path f, int bufferSize)
                            throws IOException
                    {
                        return wrapStream(super.open(f, bufferSize));
                    }

                    @Override
                    public FSDataInputStream open(Path f)
                            throws IOException
                    {
                        return wrapStream(super.open(f));
                    }
                };
            }
        };
    }

    private FSDataInputStream wrapStream(FSDataInputStream fsDataInputStream)
    {
        if (fsDataInputStream instanceof DFSDataInputStream) {
            try {
                return new FSDataInputStream(new SlowDatanodeSwitchingInputStream((DFSDataInputStream) fsDataInputStream));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            return fsDataInputStream;
        }
    }

    private static void setUnfavoredNodes(DFSDataInputStream dfsDataInputStream, Collection<DatanodeInfo> datanodeInfos)
    {
        if (setUnfavoredNodesMethod != null) {
            try {
                setUnfavoredNodesMethod.invoke(dfsDataInputStream, datanodeInfos);
            }
            catch (IllegalAccessException ignored) {
            }
            catch (InvocationTargetException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
    }

    // TODO: consider adding a limit on how often a stream can be marked with unfavored nodes
    @NotThreadSafe
    private class SlowDatanodeSwitchingInputStream
            extends FilterInputStream
            implements Seekable, PositionedReadable
    {
        private final DFSDataInputStream dfsDataInputStream;
        private ByteRateDecayCounter streamByteRate = new ByteRateDecayCounter(streamRateDecayAlpha);

        private SlowDatanodeSwitchingInputStream(DFSDataInputStream in)
        {
            super(in);
            dfsDataInputStream = in;
            // Initialize stream with unfavored nodes before the first read call
            setUnfavoredNodes(dfsDataInputStream, unfavoredNodes.asMap().keySet());
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            long start = System.nanoTime();
            int result = in.read(b, off, len);

            if (result > minMonitorThreshold.toBytes()) {
                Duration duration = Duration.nanosSince(start);
                long byteRate = (long) (result / duration.convertTo(SECONDS));
                globalDistribution.add(byteRate, result);
                streamByteRate.add(result, (long) duration.convertTo(TimeUnit.MILLISECONDS));

                if (streamByteRate.getMillis() > minStreamSamplingTime.toMillis()) {
                    List<Long> percentileValues = globalDistribution.getPercentiles(ImmutableList.of(slowStreamPercentile / 100.0, 0.50));
                    if (streamByteRate.getBytesPerSecond() < minStreamRate.toBytes() || globalDistribution.getCount() > minGlobalSamples && streamByteRate.getBytesPerSecond() < percentileValues.get(0)) {
                        log.warn("Stream ID: %s, Slow Node: %s, Current Block ID: %s, Avg File Stream Rate: %sB/s, Stream Data: %sB, Stream Time: %ss, Last Read Rate: %sB/s, Last Read Size %sB, p%s Rate: %sB/s, p50 Rate: %sB/s, Read Distribution Count: %s",
                                System.identityHashCode(this),
                                dfsDataInputStream.getCurrentDatanode().getName(),
                                dfsDataInputStream.getCurrentBlock().getBlockName(),
                                (long) streamByteRate.getBytesPerSecond(),
                                (long) streamByteRate.getBytes(),
                                streamByteRate.getMillis() / 1000,
                                byteRate,
                                result,
                                slowStreamPercentile,
                                percentileValues.get(0),
                                percentileValues.get(1),
                                globalDistribution.getCount());
                        DatanodeInfo currentDatanode = dfsDataInputStream.getCurrentDatanode();
                        unfavoredNodes.put(currentDatanode, currentDatanode);
                        setUnfavoredNodes(dfsDataInputStream, unfavoredNodes.asMap().keySet());
                        // Reset the bytes rate counter when we switch to the new node
<<<<<<< HEAD
                        byteRateDecayCounter = new ByteRateDecayCounter(streamRateDecayAlpha);
=======
                        streamByteRate = new ByteRateDecayCounter(streamRateDecayAlpha);
                        slowNodeCounter.update(1);
>>>>>>> 783d2d0... switcher updates
                    }
                }
            }

            return result;
        }

        // --- Add the below methods to be compatible with the implicitly required Seekable and PositionReadable interfaces ---

        @Override
        public synchronized void seek(long desired)
                throws IOException
        {
            ((Seekable) in).seek(desired);
        }

        @Override
        public long getPos()
                throws IOException
        {
            return ((Seekable) in).getPos();
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            return ((PositionedReadable) in).read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            ((PositionedReadable) in).readFully(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException
        {
            ((PositionedReadable) in).readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public List<ByteBuffer> readFullyScatterGather(long position, int length)
                throws IOException
        {
            return ((PositionedReadable) in).readFullyScatterGather(position, length);
        }

        @Override
        public boolean seekToNewSource(long targetPos)
                throws IOException
        {
            return ((Seekable) in).seekToNewSource(targetPos);
        }
    }

    private static class ByteRateDecayCounter
    {
        private final DecayCounter bytesCounter;
        private final DecayCounter millisCounter;

        private ByteRateDecayCounter(double alpha)
        {
            bytesCounter = new DecayCounter(alpha);
            millisCounter = new DecayCounter(alpha);
        }

        public void add(long bytes, long millis)
        {
            bytesCounter.add(bytes);
            millisCounter.add(millis);
        }

        public double getBytesPerSecond()
        {
            return bytesCounter.getCount() / (millisCounter.getCount() / 1000);
        }

        public double getBytes()
        {
            return bytesCounter.getCount();
        }

        public double getMillis()
        {
            return millisCounter.getCount();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("bytesPerSecond", getBytesPerSecond())
                    .add("bytes", getBytes())
                    .add("millis", getMillis())
                    .toString();
        }
    }
}
