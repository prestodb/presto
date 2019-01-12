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
package io.prestosql.rcfile;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.lang.reflect.Constructor;

public class HadoopCodecFactory
        implements RcFileCodecFactory
{
    private final ClassLoader classLoader;

    public HadoopCodecFactory(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public RcFileCompressor createCompressor(String codecName)
    {
        CompressionCodec codec = createCompressionCodec(codecName);
        return new HadoopCompressor(codec);
    }

    @Override
    public RcFileDecompressor createDecompressor(String codecName)
    {
        CompressionCodec codec = createCompressionCodec(codecName);
        return new HadoopDecompressor(codec);
    }

    private CompressionCodec createCompressionCodec(String codecName)
    {
        try {
            Class<? extends CompressionCodec> codecClass = classLoader.loadClass(codecName).asSubclass(CompressionCodec.class);
            Constructor<? extends CompressionCodec> constructor = codecClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            CompressionCodec codec = constructor.newInstance();
            if (codec instanceof Configurable) {
                // Hadoop is crazy... you have to give codecs an empty configuration or they throw NPEs
                // but you need to make sure the configuration doesn't "load" defaults or it spends
                // forever loading XML with no useful information
                ((Configurable) codec).setConf(new Configuration(false));
            }
            return codec;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Unknown codec: " + codecName, e);
        }
    }
}
