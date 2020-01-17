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

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.reflect.Type;

import static java.util.Objects.requireNonNull;

class SmileCodecProvider
        implements Provider<SmileCodec<?>>
{
    private final Type type;
    private SmileCodecFactory smileCodecFactory;

    public SmileCodecProvider(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Inject
    public void setSmileCodecFactory(SmileCodecFactory smileCodecFactory)
    {
        this.smileCodecFactory = smileCodecFactory;
    }

    @Override
    public SmileCodec<?> get()
    {
        return smileCodecFactory.smileCodec(type);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SmileCodecProvider that = (SmileCodecProvider) o;

        if (!type.equals(that.type)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode();
    }
}
