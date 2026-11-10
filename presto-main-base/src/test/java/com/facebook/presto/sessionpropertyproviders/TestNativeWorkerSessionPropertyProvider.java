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
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.testng.annotations.Test;

import static com.facebook.presto.sessionpropertyproviders.NativeWorkerSessionPropertyProvider.NATIVE_RPC_CONGESTION_ASYNC_GROWTH_MAX_WINDOW;
import static com.facebook.presto.sessionpropertyproviders.NativeWorkerSessionPropertyProvider.NATIVE_RPC_CONGESTION_BATCH_START_WINDOW;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestNativeWorkerSessionPropertyProvider
{
    @Test
    public void testRpcCongestionBatchStartWindowValidation()
    {
        PropertyMetadata<?> property = new NativeWorkerSessionPropertyProvider(
                new FeaturesConfig().setNativeExecutionEnabled(true))
                .getSessionProperties().stream()
                .filter(metadata -> metadata.getName().equals(NATIVE_RPC_CONGESTION_BATCH_START_WINDOW))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Property is not registered"));

        assertEquals(property.decode(0L), 0L);
        assertEquals(property.decode(16L), 16L);

        PrestoException exception = expectThrows(PrestoException.class, () -> property.decode(-1L));
        assertEquals(exception.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
    }

    @Test
    public void testRpcCongestionAsyncGrowthMaxWindowValidation()
    {
        PropertyMetadata<?> property = new NativeWorkerSessionPropertyProvider(
                new FeaturesConfig().setNativeExecutionEnabled(true))
                .getSessionProperties().stream()
                .filter(metadata -> metadata.getName().equals(NATIVE_RPC_CONGESTION_ASYNC_GROWTH_MAX_WINDOW))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Property is not registered"));

        assertEquals(property.decode(0L), 0L);
        assertEquals(property.decode(16L), 16L);

        PrestoException exception = expectThrows(PrestoException.class, () -> property.decode(-1L));
        assertEquals(exception.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
    }
}
