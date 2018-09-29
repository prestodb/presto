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
package com.facebook.presto.kafkastream;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import javax.annotation.Nullable;

/**
 *
 */
public class KeyTransformer
        implements Transformer<String, String, KeyValue<String, String>>
{
    private static final String BASE_ID_JSON_PATH = "$.data[0].Id";
    private ProcessorContext context;

    /*
     * (non-Javadoc)
     * @see org.apache.kafka.streams.kstream.Transformer#init(org.apache.kafka.
     * streams.processor.ProcessorContext)
     */
    @Override
    public void init(ProcessorContext context)
    {
        this.context = context;
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.kafka.streams.kstream.Transformer#transform(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public KeyValue<String, String> transform(String key, String value)
    {
        String baseId = JsonPath.read(value, BASE_ID_JSON_PATH);
        System.out.println("BaseId---------> " + baseId);
        Long newValue = context.offset();
        KeyValue<String, String> keyVal =
                new KeyValue<String, String>(baseId, newValue.toString());
        return keyVal;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.kafka.streams.kstream.Transformer#punctuate(long)
     */
    @Override
    @Nullable
    public KeyValue<String, String> punctuate(long timestamp)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.kafka.streams.kstream.Transformer#close()
     */
    @Override
    public void close()
    {
        // do nothing for now
    }
}
