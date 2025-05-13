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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.ByteBuffer;
import java.util.Properties;

public interface KafkaConsumerManager
{
    default KafkaConsumer<ByteBuffer, ByteBuffer> createConsumer(String threadName, HostAddress hostAddress)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(KafkaPlugin.class.getClassLoader())) {
            return new KafkaConsumer<>(configure(threadName, hostAddress));
        }
    }

    Properties configure(String threadName, HostAddress hostAddress);
}
