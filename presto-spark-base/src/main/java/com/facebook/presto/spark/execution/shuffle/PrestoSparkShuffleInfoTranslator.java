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
package com.facebook.presto.spark.execution.shuffle;

import com.facebook.presto.Session;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleReadDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleWriteDescriptor;
import com.facebook.presto.spark.execution.PrestoSparkShuffleReadInfo;
import com.facebook.presto.spark.execution.PrestoSparkShuffleWriteInfo;

/**
 * PrestoSparkShuffleInfoTranslator is used to translate the {@link PrestoSparkShuffleWriteDescriptor} and
 * {@link PrestoSparkShuffleReadDescriptor} to a serializable format which can be carried over to the Native execution process.
 * These serialized formats could be either {@link PrestoSparkShuffleWriteDescriptor}/{@link PrestoSparkShuffleReadDescriptor} or the
 * Json string by using the createSerializedWriteInfo or createSerializedReadInfo methods.
 */
public interface PrestoSparkShuffleInfoTranslator
{
    PrestoSparkShuffleWriteInfo createShuffleWriteInfo(Session session, PrestoSparkShuffleWriteDescriptor writeDescriptor);

    PrestoSparkShuffleReadInfo createShuffleReadInfo(Session session, PrestoSparkShuffleReadDescriptor readDescriptor);

    String createSerializedWriteInfo(PrestoSparkShuffleWriteInfo writeInfo);

    String createSerializedReadInfo(PrestoSparkShuffleReadInfo readInfo);
}
