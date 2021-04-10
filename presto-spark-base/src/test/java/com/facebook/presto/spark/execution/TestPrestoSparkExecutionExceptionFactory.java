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

import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Failure;
import com.facebook.presto.spark.classloader_interface.PrestoSparkExecutionException;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.spark.ExceptionFailure;
import org.apache.spark.SparkException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static scala.collection.JavaConversions.asScalaBuffer;

public class TestPrestoSparkExecutionExceptionFactory
{
    private static final String SPARK_EXCEPTION_STRING = "Job aborted due to stage failure: " +
            "Task 7 in stage 3.0 failed 1 times, most recent failure: " +
            "Lost task 7.0 in stage 3.0 (TID 3, localhost): ";

    private PrestoSparkExecutionExceptionFactory factory;

    @BeforeClass
    public void setup()
    {
        factory = new PrestoSparkExecutionExceptionFactory(jsonCodec(ExecutionFailureInfo.class));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        factory = null;
    }

    @Test
    public void testRoundTrip()
    {
        String causeMessage = "cause message";
        IOException cause = new IOException(causeMessage);
        String suppressedMessage = "suppressed message";
        IllegalArgumentException suppressed = new IllegalArgumentException(suppressedMessage);
        String message = "presto exception message";
        PrestoException prestoException = new PrestoException(NOT_SUPPORTED, message, cause);
        prestoException.addSuppressed(suppressed);

        PrestoSparkExecutionException executionException = factory.toPrestoSparkExecutionException(prestoException);

        Optional<ExecutionFailureInfo> failure = factory.extractExecutionFailureInfo(executionException);
        assertTrue(failure.isPresent());
        assertFailure(failure.get().toFailure(), prestoException);

        ExceptionFailure exceptionFailure = new ExceptionFailure(executionException, asScalaBuffer(ImmutableList.of()));
        SparkException sparkException = new SparkException(SPARK_EXCEPTION_STRING + exceptionFailure.toErrorString());

        failure = factory.extractExecutionFailureInfo(sparkException);
        assertTrue(failure.isPresent());
        assertFailure(failure.get().toFailure(), prestoException);
    }

    private static void assertFailure(Failure failure, Throwable expected)
    {
        ErrorCode expectedErrorCode = expected instanceof PrestoException ? ((PrestoException) expected).getErrorCode() : GENERIC_INTERNAL_ERROR.toErrorCode();
        assertEquals(failure.getErrorCode(), expectedErrorCode);
        assertEquals(failure.getType(), expected.getClass().getName());
        assertEquals(failure.getMessage(), expected.getMessage());
        if (expected.getCause() != null) {
            assertNotNull(failure.getCause());
            assertFailure((Failure) failure.getCause(), expected.getCause());
        }
        if (expected.getSuppressed() != null) {
            assertNotNull(failure.getSuppressed());
            assertEquals(failure.getSuppressed().length, expected.getSuppressed().length);
            for (int i = 0; i < expected.getSuppressed().length; i++) {
                assertFailure((Failure) failure.getSuppressed()[i], expected.getSuppressed()[i]);
            }
        }
    }
}
