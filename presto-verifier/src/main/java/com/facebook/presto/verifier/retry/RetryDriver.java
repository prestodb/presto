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
package com.facebook.presto.verifier.retry;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.verifier.framework.QueryException;
import io.airlift.units.Duration;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RetryDriver<E extends RuntimeException>
{
    private static final Logger log = Logger.get(RetryDriver.class);

    private final int maxAttempts;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final double scaleFactor;
    private final Predicate<E> retryPredicate;
    private final Class<E> exceptionClass;
    private final Consumer<E> exceptionCallback;

    public RetryDriver(
            RetryConfig config,
            Predicate<E> retryPredicate,
            Class<E> exceptionClass,
            Consumer<E> exceptionCallback)
    {
        this.maxAttempts = config.getMaxAttempts();
        this.minBackoffDelay = requireNonNull(config.getMinBackoffDelay(), "minBackoffDelay is null");
        this.maxBackoffDelay = requireNonNull(config.getMaxBackoffDelay(), "maxBackoffDelay is null");
        this.scaleFactor = config.getScaleFactor();
        this.retryPredicate = requireNonNull(retryPredicate, "retryPredicate is null");
        this.exceptionClass = requireNonNull(exceptionClass, "exceptionClass is null");
        this.exceptionCallback = requireNonNull(exceptionCallback, "exceptionCallback is null");
    }

    @SuppressWarnings("unchecked")
    public <V> V run(String callableName, RetryOperation<V> operation)
    {
        int attempt = 1;
        while (true) {
            try {
                return operation.run();
            }
            catch (Exception e) {
                if (!exceptionClass.isInstance(e)) {
                    throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }
                E qe = (E) e;
                exceptionCallback.accept(qe);
                if (attempt >= maxAttempts || !retryPredicate.test(qe)) {
                    throw qe;
                }

                attempt++;
                int delayMillis = (int) min(minBackoffDelay.toMillis() * pow(scaleFactor, attempt - 1), maxBackoffDelay.toMillis());
                int jitterMillis = ThreadLocalRandom.current().nextInt(max(1, (int) (delayMillis * 0.1)));
                log.info(
                        "Failed on executing %s with attempt %d. Retry after %sms. Cause: %s",
                        callableName,
                        attempt - 1,
                        delayMillis,
                        qe.getMessage());

                try {
                    MILLISECONDS.sleep(delayMillis + jitterMillis);
                }
                catch (InterruptedException ie) {
                    currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    public interface RetryOperation<V>
    {
        V run()
                throws QueryException;
    }
}
