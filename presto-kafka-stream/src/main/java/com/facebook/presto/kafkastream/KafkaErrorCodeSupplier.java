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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Error code supplier for presto. Presto returns the error code
 * to client. Here, error code range starts from 0x7F01_0000 (2130771968)
 */
public class KafkaErrorCodeSupplier
        implements ErrorCodeSupplier
{
    private static final int ERROR_CODE_RANGE_START = 0x7F01_0000;
    private final ErrorCode errorCode;

    /**
     * All the error codes which needs to be returned from this connector.
     */
    public enum KafkaError
    {
        // Kafka client creation  error
        KAFKA_CONSUMER_CREATION_ERROR(0, EXTERNAL),
        KAFKA_OFFSET_SEEK_ERROR(1, EXTERNAL),

        // Connector errors
        // Validation errors
        KAFKA_CONNECTOR_VALIDATION_ERROR(501, INTERNAL_ERROR),
        KAFKA_CONNECTOR_INTERNAL_ERROR(503, INTERNAL_ERROR);

        private final int code;
        private final ErrorType errorType;

        KafkaError(int code, ErrorType errorType)
        {
            this.code = code;
            this.errorType = errorType;
        }

        public int getCode()
        {
            return code;
        }

        public ErrorType getErrorType()
        {
            return errorType;
        }
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public KafkaErrorCodeSupplier(KafkaError error)
    {
        requireNonNull(error);

        errorCode = new ErrorCode(error.getCode() + ERROR_CODE_RANGE_START, error.name(), error.getErrorType());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
