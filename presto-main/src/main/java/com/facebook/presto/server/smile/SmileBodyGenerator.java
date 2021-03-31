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
package com.facebook.presto.server.smile;

import com.facebook.airlift.http.client.StaticBodyGenerator;
import com.facebook.airlift.json.smile.SmileCodec;
import com.google.common.annotations.Beta;

@Beta
public class SmileBodyGenerator<T>
        extends StaticBodyGenerator
{
    public static <T> SmileBodyGenerator<T> smileBodyGenerator(SmileCodec<T> smileCodec, T instance)
    {
        return new SmileBodyGenerator<>(smileCodec, instance);
    }

    private SmileBodyGenerator(SmileCodec<T> smileCodec, T instance)
    {
        super(smileCodec.toBytes(instance));
    }
}
