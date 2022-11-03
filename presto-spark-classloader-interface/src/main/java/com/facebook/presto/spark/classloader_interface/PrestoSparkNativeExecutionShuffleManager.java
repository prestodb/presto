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
package com.facebook.presto.spark.classloader_interface;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static java.lang.String.format;

/*
 * {@link PrestoSparkNativeExecutionShuffleManager} is the shuffle manager implementing the Spark shuffle manager interface specifically for native execution. The reasons we have this
 *  new shuffle manager are:
 * 1. To bypass calling into Spark java shuffle writer/reader since the actual shuffle read/write will happen in C++ side. In PrestoSparkNativeExecutionShuffleManager, we registered
 *    a pair of no-op shuffle reader/writer to hook-up with regular Spark shuffle workflow.
 * 2. To capture the shuffle metadata (eg. {@link ShuffleHandle}) for later use. These metadata are only available during shuffle writer creation internally which is beyond the whole
 *    Presto-Spark native execution flow. By using the {@link PrestoSparkNativeExecutionShuffleManager}, we capture and store these metadata inside the shuffle manager and provide
 *    the APIs to allow native execution runtime access.
 * */
public class PrestoSparkNativeExecutionShuffleManager
        implements ShuffleManager
{
    private final Map<Integer, ShuffleHandle> shuffleDependencyMap = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> shuffleNumMaps = new ConcurrentHashMap<>();
    private final ShuffleManager fallbackShuffleManager;
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";

    public PrestoSparkNativeExecutionShuffleManager(SparkConf conf)
    {
        fallbackShuffleManager = instantiateClass(conf.get(FALLBACK_SPARK_SHUFFLE_MANAGER), conf);
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    private static <T> T instantiateClass(String className, SparkConf conf)
    {
        try {
            return (T) (Class.forName(className).getConstructor(SparkConf.class).newInstance(conf));
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(format("%s class not found", className), e);
        }
    }

    @Override
    public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency)
    {
        shuffleNumMaps.put(shuffleId, numMaps);
        return fallbackShuffleManager.registerShuffle(shuffleId, numMaps, dependency);
    }

    @Override
    public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId, TaskContext context)
    {
        shuffleDependencyMap.put(mapId, handle);
        int shuffleId = handle.shuffleId();
        return new EmptyShuffleWriter<>(getNumOfPartitions(shuffleId));
    }

    @Override
    public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle, int startPartition, int endPartition, TaskContext context)
    {
        return new EmptyShuffleReader<>();
    }

    @Override
    public boolean unregisterShuffle(int shuffleId)
    {
        fallbackShuffleManager.unregisterShuffle(shuffleId);
        return true;
    }

    @Override
    public ShuffleBlockResolver shuffleBlockResolver()
    {
        return fallbackShuffleManager.shuffleBlockResolver();
    }

    @Override
    public void stop()
    {
        fallbackShuffleManager.stop();
    }

    /*
     * This method can only be called inside Rdd's compute method otherwise the shuffleDependencyMap may not contain corresponding ShuffleHandle object.
     * The reason is that in Spark's ShuffleMapTask, it's guaranteed to call writer.getWriter(handle, mapId, context) first before calling the Rdd.compute()
     * method, therefore, the ShuffleHandle object will always be added to shuffleDependencyMap in getWriter before Rdd.compute().
     */
    public Optional<ShuffleHandle> getShuffleHandle(int partitionId)
    {
        return Optional.ofNullable(shuffleDependencyMap.getOrDefault(partitionId, null));
    }

    public int getNumOfPartitions(int shuffleId)
    {
        if (!shuffleNumMaps.containsKey(shuffleId)) {
            throw new RuntimeException(format("shuffleId=[%s] is not registered", shuffleId));
        }
        return shuffleNumMaps.get(shuffleId);
    }

    static class EmptyShuffleReader<K, V>
            implements ShuffleReader<K, V>
    {
        @Override
        public Iterator<Product2<K, V>> read()
        {
            return emptyScalaIterator();
        }
    }

    static class EmptyShuffleWriter<K, V>
            extends ShuffleWriter<K, V>
    {
        private final long[] mapStatus;

        public EmptyShuffleWriter(int totalMapStages)
        {
            this.mapStatus = new long[totalMapStages];
        }

        @Override
        public void write(Iterator<Product2<K, V>> records)
                throws IOException
        {
            if (records.hasNext()) {
                throw new RuntimeException("EmptyShuffleWriter can only take empty write input.");
            }
        }

        @Override
        public Option<MapStatus> stop(boolean success)
        {
            BlockManager blockManager = SparkEnv.get().blockManager();
            return Option.apply(MapStatus$.MODULE$.apply(blockManager.blockManagerId(), mapStatus));
        }
    }
}
