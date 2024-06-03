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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.relation.RowExpression;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class NativeExpressionModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);
        jsonBinder(binder).addDeserializerBinding(RowExpression.class).to(RowExpressionDeserializer.class);
        jsonBinder(binder).addSerializerBinding(RowExpression.class).to(RowExpressionSerializer.class);
        jsonCodecBinder(binder).bindListJsonCodec(RowExpression.class);
        binder.bind(NativeBatchRowExpressionInterpreterProvider.class).in(Scopes.SINGLETON);
    }
}
