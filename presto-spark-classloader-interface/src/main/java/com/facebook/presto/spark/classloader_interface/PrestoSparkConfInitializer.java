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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class PrestoSparkConfInitializer
{
    private static final String INITIALIZED_MARKER = "presto.spark.conf.initialized";

    private PrestoSparkConfInitializer() {}

    public static void initialize(SparkConf sparkConf)
    {
        if (sparkConf.get(INITIALIZED_MARKER, null) != null) {
            throw new IllegalArgumentException("SparkConf is already initialized");
        }
        registerKryoClasses(sparkConf);
        sparkConf.set(INITIALIZED_MARKER, "true");
    }

    private static void registerKryoClasses(SparkConf sparkConf)
    {
        sparkConf.registerKryoClasses(new Class[] {
                MutablePartitionId.class,
                PrestoSparkMutableRow.class,
                PrestoSparkSerializedPage.class,
                SerializedPrestoSparkTaskDescriptor.class,
                SerializedTaskInfo.class,
                PrestoSparkShuffleStats.class,
                PrestoSparkStorageHandle.class,
                SerializedPrestoSparkTaskSource.class
        });
    }

    public static void checkInitialized(SparkContext sparkContext)
    {
        SparkConf sparkConf = sparkContext.getConf();
        if (sparkConf.get(INITIALIZED_MARKER, null) == null) {
            throw new IllegalArgumentException("SparkConf must be initialized with PrestoSparkConfInitializer.initialize before creating SparkContext");
        }
    }
}
