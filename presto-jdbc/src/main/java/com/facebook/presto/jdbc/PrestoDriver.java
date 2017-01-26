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
package com.facebook.presto.jdbc;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.jdbc.ConnectionProperties.USER;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Integer.parseInt;

public class PrestoDriver
        implements Driver, Closeable
{
    static final String DRIVER_NAME = "Presto JDBC Driver";
    static final String DRIVER_VERSION;
    static final int DRIVER_VERSION_MAJOR;
    static final int DRIVER_VERSION_MINOR;

    private static final String DRIVER_URL_START = "jdbc:presto:";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final JettyIoPool jettyIoPool;

    private final ReferenceCountingLoadingCache<HttpClientCreator, QueryExecutor> queryExecutorCache;

    static {
        String version = nullToEmpty(PrestoDriver.class.getPackage().getImplementationVersion());
        Matcher matcher = Pattern.compile("^(\\d+)\\.(\\d+)($|[.-])").matcher(version);
        if (!matcher.find()) {
            DRIVER_VERSION = "unknown";
            DRIVER_VERSION_MAJOR = 0;
            DRIVER_VERSION_MINOR = 0;
        }
        else {
            DRIVER_VERSION = version;
            DRIVER_VERSION_MAJOR = parseInt(matcher.group(1));
            DRIVER_VERSION_MINOR = parseInt(matcher.group(2));
        }

        try {
            DriverManager.registerDriver(new PrestoDriver());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /*
     * The relationship between the PrestoDriver, QueryExecutors, and
     * PrestoConnections is complicated:
     *
     * PrestoConnection <-many:one-> QueryExecutor <-many:one-> PrestoDriver
     *                               (HTTP client)
     *
     * - A PrestoConnection represents the outside world's view of a JDBC
     * connection to a Presto server.
     * - A PrestoConnection contains a QueryExecutor, which is responsible
     * for actually executing queries against the Presto server.
     * - QueryExecutor contains a reference to a driver-wide pool of execution
     * resources, and an HTTP client that executes requests using the resources
     * in that pool.
     *
     * An HTTP client is expensive to create and destroy, and we would like to
     * share it across as many PrestoConnections as possible. If every
     * connection could use the same HTTP client, life would be simple.
     * Unfortunately, some things that can be specified at the PrestoConnection
     * level must be configured at the HTTP client level. The obvious case of
     * this is the SSL/TLS trust store configuration: An HTTP client has to be
     * configured with the appropriate trust store to make SSL connections to a
     * Presto server.
     *
     * The PrestoDriver deals in QueryExecutors, which have a 1:1 relationship
     * with HTTP clients. In order to create many PrestoConnections with the
     * same QueryExecutor, the PrestoDriver maintains a cache of
     * QueryExecutors.
     *
     * This leaves us having to solve one of the two hard problems in computer
     * science: cache invalidation, which in this case, is closely intertwined
     * with the need to eventually close the HTTP client contained in the
     * cached QueryExecutor.
     *
     * Conceptually, the flow is simple:
     * driver.connect() returns either
     * 1) A cached QueryExecutor that has an HTTP client that satisfies the
     *    requested connection properties.
     * 2) A new QueryExecutor with a new HTTP client if no cached
     *    QueryExecutor is satisfactory.
     *
     * connection.close() calls queryExecutor.close()
     * If this is the last reference to the queryExecutor, close the HTTP
     * client and invalidate() queryExecutor from the cache.
     *
     * Now we need a reference count. And things get hairy, because the cache
     * updates and reference count manipulation need to happen atomically.
     *
     * The solution is to protect the refcounts by synchronizing on the cache
     * and incrementing and decrementing the refcount while holding the cache's
     * monitor. This probably doesn't scale to gazillions of operations per
     * second, but we're operating on the assumption that it doesn't have to.
     * Presumably anybody who wants a connection probably also wants to use it,
     * and the lifetime of a connection is dominated by the time it takes to
     * actually run queries.
     *
     * This logic is encapsulated in ReferenceCountingLoadingCache. get() and
     * close() operations call acquire() and release(), respectively.
     *
     * The last wrinkle is that we need to retain released() QueryExecutors
     * for some short time period to handle cases where somebody releases the
     * last reference to a QE, but is going to reacquire it immediately. This
     * requires scheduling cache invalidation into the future, which adds yet
     * more joy to our lives, as detailed below.
     *
     * The cleaner count handles the following situation:
     *
     * Thread T1 has a reference on V for key K.
     * T1 calls release(K1) scheduling future F1 and increments cleaner count -> 1
     * time passes.
     * A daemon thread starts executing F1, but does not acquire the lock
     * Thread T2 calls acquire() and acquires the lock.
     *   acquire() increments the refcount
     *   acquire tries, but fails to cancel F1 because F1 isn't is running
     *     but is not interruptible.
     *   acquire() releases the lock
     *
     * At this point 2 things can happen
     * 1) F1 resumes executing. At this point the refcount is sufficient to
     *    ensure correctness
     * 2) T2 calls release(K1)
     *      release() acquires the lock.
     *      release schedules a cleanup F2 and increments cleaner count -> 2
     *      release fails to cancel F1 for the same reason acquire did.
     *      release() releases the lock.
     *    F1 resumes executing and acquires the lock.
     *      releaseForCleaning() decrements cleaner count -> 1
     *      nothing further happens.
     *    more time passes.
     *    F2 executes
     *      releaseForCleaning() decrements cleaner count -> 0
     *      refcount is 0
     *      K1 is invalidated from the cache, and V1 is disposed.
     *
     * Without the cleaner count, we run the risk that some thread T3 calls
     * acquire(K1), loading V2 while more time passes. F2 is still using V1's
     * Holder, which has a refcount of zero. F1 then invalidates K1, which
     * disposes of V2 while it's still being used by T3. Havoc ensues.
     *
     * Instead of keeping a cleaner count, we could have a boolean flag in the
     * Holder that tracks whether or not the value has been invalidated. The
     * cleaner count has the advantage of ensuring the desired retention period
     * as well as guarding against the above situation, so it's all around
     * better.
     *
     * The other, other approach would be to set an 'invalidate after' time
     * in the holder. Now we have to worry about clock resolution, jitter, and
     * monotonicity. No thanks; somebody did that work for the ScheduledFuture/
     * ScheduledExecutorService, let's take advantage of that.
     *
     * The last case of interest is the process that is using PrestoDriver
     * calling prestoDriver.close(). This invalidates all of the cached
     * QueryExecutors regardless of their refcount by calling the cache's
     * close() method, and in doing so closes their HTTP clients. If anybody is
     * still trying to use a PrestoConnection created by the driver, they're
     * SOL.
     */

    public PrestoDriver()
    {
        this.jettyIoPool = new JettyIoPool("presto-jdbc", new JettyIoPoolConfig());
        this.queryExecutorCache = ReferenceCountingLoadingCache.<HttpClientCreator, QueryExecutor>builder().build(
                httpClientCreator -> QueryExecutor.create(httpClientCreator.create(QueryExecutor.baseClientConfig())),
                QueryExecutor::close);
    }

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true)) {
            queryExecutorCache.close();
            jettyIoPool.close();
        }
    }

    @Override
    public Connection connect(String url, Properties driverProperties)
            throws SQLException
    {
        if (closed.get()) {
            throw new SQLException("Already closed");
        }

        if (!acceptsURL(url)) {
            return null;
        }

        PrestoConnectionConfig uri = new PrestoConnectionConfig(url, driverProperties);
        String user = USER.getValue(uri.getConnectionProperties()).get();

        HttpClientCreator clientCreator = uri.getCreator(DRIVER_NAME + "/" + DRIVER_VERSION, jettyIoPool);

        QueryExecutor queryExecutor = queryExecutorCache.acquire(clientCreator);
        return new PrestoConnection(uri, user, queryExecutor, () -> queryExecutorCache.release(clientCreator));
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties driverProperties)
            throws SQLException
    {
        PrestoConnectionConfig uri = new PrestoConnectionConfig(url, driverProperties);
        ImmutableList.Builder<DriverPropertyInfo> result = ImmutableList.builder();

        Properties mergedProperties = uri.getConnectionProperties();
        for (ConnectionProperty property : ConnectionProperties.allOf()) {
            result.add(property.getDriverPropertyInfo(mergedProperties));
        }

        return result.build().toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion()
    {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }
}
