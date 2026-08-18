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
package com.facebook.presto.flightshim;

import com.facebook.plugin.arrow.ArrowErrorCode;
import com.facebook.presto.spi.PrestoException;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightStatusCode;
import org.testng.annotations.Test;

import static com.facebook.presto.flightshim.FlightShimErrors.ARROW_STATUS_KEY;
import static com.facebook.presto.flightshim.FlightShimErrors.ERROR_CODE_KEY;
import static com.facebook.presto.flightshim.FlightShimErrors.ERROR_NAME_KEY;
import static com.facebook.presto.flightshim.FlightShimErrors.ERROR_RETRIABLE_KEY;
import static com.facebook.presto.flightshim.FlightShimErrors.ERROR_TYPE_KEY;
import static com.facebook.presto.flightshim.FlightShimErrors.GRPC_STATUS_DETAILS_KEY;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFlightShimErrors
{
    @Test
    public void testUserError()
    {
        CallStatus status = FlightShimErrors.toCallStatus(new PrestoException(NOT_FOUND, "table dropped"));
        assertEquals(status.metadata().get(ERROR_NAME_KEY), "NOT_FOUND");
        assertEquals(status.metadata().get(ERROR_CODE_KEY), Integer.toString(NOT_FOUND.toErrorCode().getCode()));
        assertEquals(status.metadata().get(ERROR_TYPE_KEY), "USER_ERROR");
        assertEquals(status.metadata().get(ERROR_RETRIABLE_KEY), "false");
    }

    @Test
    public void testRetriableExternalErrorInCauseChain()
    {
        CallStatus status = FlightShimErrors.toCallStatus(new RuntimeException(
                new PrestoException(ArrowErrorCode.ARROW_FLIGHT_UNAVAILABLE_ERROR, "remote down")));
        assertEquals(status.metadata().get(ERROR_RETRIABLE_KEY), "true");
        String details = new String(status.metadata().getByte(GRPC_STATUS_DETAILS_KEY), UTF_8);
        assertTrue(details.contains(ERROR_NAME_KEY + "=ARROW_FLIGHT_UNAVAILABLE_ERROR"));
        assertTrue(details.contains(ERROR_TYPE_KEY + "=EXTERNAL"));
        assertTrue(details.contains(ERROR_RETRIABLE_KEY + "=true"));
    }

    @Test
    public void testInsufficientResources()
    {
        CallStatus status = FlightShimErrors.toCallStatus(
                new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "out of memory"));
        assertEquals(status.metadata().get(ERROR_TYPE_KEY), "INSUFFICIENT_RESOURCES");
    }

    @Test
    public void testUnclassifiedErrorFallsBackToOpaqueInternalError()
    {
        CallStatus status = FlightShimErrors.toCallStatus(new NullPointerException("bug"));
        assertEquals(status.code(), FlightStatusCode.INTERNAL);
        assertEquals(status.metadata().get(ERROR_NAME_KEY), "GENERIC_INTERNAL_ERROR");
        assertEquals(status.metadata().get(ERROR_TYPE_KEY), "INTERNAL_ERROR");
    }

    @Test
    public void testStatusIsInternalForEveryErrorType()
    {
        assertStatusIsInternal(new PrestoException(NOT_FOUND, "user"));
        assertStatusIsInternal(new PrestoException(GENERIC_INTERNAL_ERROR, "internal"));
        assertStatusIsInternal(new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "resources"));
        assertStatusIsInternal(new PrestoException(ArrowErrorCode.ARROW_FLIGHT_UNAVAILABLE_ERROR, "external"));
    }

    private static void assertStatusIsInternal(Throwable throwable)
    {
        CallStatus status = FlightShimErrors.toCallStatus(throwable);
        assertEquals(status.code(), FlightStatusCode.INTERNAL);
        assertEquals(status.metadata().get(ARROW_STATUS_KEY), "5");
    }
}
