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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.plugin.arrow.testingServer.TestingArrowFlightRequest;
import com.facebook.plugin.arrow.testingServer.TestingArrowFlightResponse;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class TestingArrowModule
        implements Module
{
    private final boolean nativeExecution;

    public TestingArrowModule(boolean nativeExecution)
    {
        this.nativeExecution = nativeExecution;
    }

    @Override
    public void configure(Binder binder)
    {
        // Concrete implementation of the BaseFlightClientHandler
        binder.bind(BaseArrowFlightClientHandler.class).to(TestingArrowFlightClientHandler.class).in(Scopes.SINGLETON);
        // Override the ArrowBlockBuilder with an implementation that handles h2 types, skip for native
        if (!nativeExecution) {
            binder.bind(ArrowBlockBuilder.class).to(TestingArrowBlockBuilder.class).in(Scopes.SINGLETON);
        }
        // Request/response objects
        jsonCodecBinder(binder).bindJsonCodec(TestingArrowFlightRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(TestingArrowFlightResponse.class);
    }
}
