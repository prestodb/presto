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

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Attaches Presto error metadata to Flight call statuses so native clients can
 * reconstruct the original error code. The Flight status itself is always
 * INTERNAL; attribution travels in the trailers.
 */
public final class FlightShimErrors
{
    static final String ERROR_NAME_KEY = "presto-error-name";
    static final String ERROR_CODE_KEY = "presto-error-code";
    static final String ERROR_TYPE_KEY = "presto-error-type";
    static final String ERROR_RETRIABLE_KEY = "presto-error-retriable";

    // Trailers recognized by Arrow C++ clients; the binary details trailer is
    // surfaced as FlightStatusDetail::extra_info only when x-arrow-status is
    // also present (see arrow/flight/transport/grpc/util_internal.cc).
    static final String ARROW_STATUS_KEY = "x-arrow-status";
    static final String GRPC_STATUS_DETAILS_KEY = "grpc-status-details-bin";

    // arrow::StatusCode::IOError. Carries no classification; it exists only to
    // satisfy the gate above, since attribution travels in the details trailer.
    private static final int ARROW_STATUS_IO_ERROR = 5;

    private FlightShimErrors() {}

    public static FlightRuntimeException toFlightException(String description, Throwable throwable)
    {
        return toCallStatus(throwable).withCause(throwable).withDescription(description).toRuntimeException();
    }

    static CallStatus toCallStatus(Throwable throwable)
    {
        return new CallStatus(FlightStatusCode.INTERNAL, null, null, toMetadata(findErrorCode(throwable)));
    }

    private static ErrorCode findErrorCode(Throwable throwable)
    {
        for (Throwable cause = throwable; cause != null; cause = (cause.getCause() == cause) ? null : cause.getCause()) {
            if (cause instanceof PrestoException) {
                return ((PrestoException) cause).getErrorCode();
            }
        }
        return GENERIC_INTERNAL_ERROR.toErrorCode();
    }

    private static ErrorFlightMetadata toMetadata(ErrorCode errorCode)
    {
        ErrorFlightMetadata metadata = new ErrorFlightMetadata();
        metadata.insert(ERROR_NAME_KEY, errorCode.getName());
        metadata.insert(ERROR_CODE_KEY, Integer.toString(errorCode.getCode()));
        metadata.insert(ERROR_TYPE_KEY, errorCode.getType().name());
        metadata.insert(ERROR_RETRIABLE_KEY, Boolean.toString(errorCode.isRetriable()));
        metadata.insert(ARROW_STATUS_KEY, Integer.toString(ARROW_STATUS_IO_ERROR));
        metadata.insert(GRPC_STATUS_DETAILS_KEY, serializeDetails(errorCode));
        return metadata;
    }

    private static byte[] serializeDetails(ErrorCode errorCode)
    {
        String payload = ERROR_NAME_KEY + "=" + errorCode.getName() + "\n" +
                ERROR_CODE_KEY + "=" + errorCode.getCode() + "\n" +
                ERROR_TYPE_KEY + "=" + errorCode.getType().name() + "\n" +
                ERROR_RETRIABLE_KEY + "=" + errorCode.isRetriable();
        return payload.getBytes(UTF_8);
    }
}
