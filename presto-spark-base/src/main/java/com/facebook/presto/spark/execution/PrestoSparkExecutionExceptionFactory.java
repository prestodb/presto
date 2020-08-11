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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.spark.classloader_interface.PrestoSparkExecutionException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNonRetryableExecutionException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkRetryableExecutionException;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorType;
import org.apache.spark.SparkException;

import javax.inject.Inject;

import java.util.Base64;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.spark.util.PrestoSparkUtils.compress;
import static com.facebook.presto.spark.util.PrestoSparkUtils.decompress;
import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;

public class PrestoSparkExecutionExceptionFactory
{
    private static final Pattern PATTERN = Pattern.compile(".*\\| ExecutionFailureInfo\\[([^\\[\\]]+)\\] \\|.*", MULTILINE | DOTALL);

    private final JsonCodec<ExecutionFailureInfo> codec;

    @Inject
    public PrestoSparkExecutionExceptionFactory(JsonCodec<ExecutionFailureInfo> codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
    }

    public PrestoSparkExecutionException toPrestoSparkExecutionException(Throwable throwable)
    {
        ExecutionFailureInfo failureInfo = toFailure(throwable);
        byte[] serialized = codec.toJsonBytes(failureInfo);
        byte[] compressed = compress(serialized);
        String encodedExecutionFailureInfo = Base64.getEncoder().encodeToString(compressed);
        if (isRetryable(failureInfo)) {
            return new PrestoSparkRetryableExecutionException(throwable.getMessage(), encodedExecutionFailureInfo, throwable);
        }
        else {
            return new PrestoSparkNonRetryableExecutionException(throwable.getMessage(), encodedExecutionFailureInfo, throwable);
        }
    }

    public Optional<ExecutionFailureInfo> extractExecutionFailureInfo(SparkException sparkException)
    {
        return extractExecutionFailureInfo(sparkException.getMessage());
    }

    public Optional<ExecutionFailureInfo> extractExecutionFailureInfo(PrestoSparkExecutionException executionException)
    {
        return extractExecutionFailureInfo(executionException.getMessage());
    }

    private Optional<ExecutionFailureInfo> extractExecutionFailureInfo(String message)
    {
        Matcher matcher = PATTERN.matcher(message);
        if (matcher.matches()) {
            String encodedFailureInfo = matcher.group(1);
            byte[] decoded = Base64.getDecoder().decode(encodedFailureInfo);
            byte[] decompressed = decompress(decoded);
            ExecutionFailureInfo failureInfo = codec.fromJson(decompressed);
            return Optional.of(failureInfo);
        }
        return Optional.empty();
    }

    private static boolean isRetryable(ExecutionFailureInfo executionFailureInfo)
    {
        ErrorCode errorCode = executionFailureInfo.getErrorCode();
        if (errorCode == null) {
            return true;
        }
        ErrorType type = errorCode.getType();
        return type == INTERNAL_ERROR || type == EXTERNAL;
    }
}
