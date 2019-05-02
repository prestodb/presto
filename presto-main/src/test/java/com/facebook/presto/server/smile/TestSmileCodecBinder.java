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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static com.facebook.presto.server.smile.SmileCodecBinder.smileCodecBinder;
import static org.testng.Assert.assertNotNull;

public class TestSmileCodecBinder
{
    @Test
    public void ignoresRepeatedBinding()
    {
        Injector injector = Guice.createInjector((Module) binder -> {
            smileCodecBinder(binder).bindSmileCodec(Integer.class);
            smileCodecBinder(binder).bindSmileCodec(Integer.class);
            binder.bind(ObjectMapper.class).annotatedWith(ForSmile.class).toProvider(SmileObjectMapperProvider.class);
            binder.bind(Dummy.class).in(Scopes.SINGLETON);
        });

        assertNotNull(injector.getInstance(Dummy.class).getCodec());
    }

    private static class Dummy
    {
        private final SmileCodec<Integer> codec;

        @Inject
        public Dummy(SmileCodec<Integer> codec)
        {
            this.codec = codec;
        }

        public SmileCodec<Integer> getCodec()
        {
            return codec;
        }
    }
}
