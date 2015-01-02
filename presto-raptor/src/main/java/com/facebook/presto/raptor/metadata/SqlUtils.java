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
package com.facebook.presto.raptor.metadata;

import com.google.common.base.Throwables;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import static java.util.concurrent.Executors.callable;

public final class SqlUtils
{
    private SqlUtils() {}

    /**
     * Run a SQL query as ignoring any constraint violations.
     * This allows idempotent inserts (equivalent to INSERT IGNORE).
     */
    public static void runIgnoringConstraintViolation(Runnable task)
    {
        try {
            runIgnoringConstraintViolation(callable(task), null);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
    }

    /**
     * Run a SQL query as ignoring any constraint violations.
     * This allows idempotent inserts (equivalent to INSERT IGNORE).
     */
    public static <T> T runIgnoringConstraintViolation(Callable<T> task)
            throws Exception
    {
        return runIgnoringConstraintViolation(task, null);
    }

    /**
     * Run a SQL query as ignoring any constraint violations.
     * This allows idempotent inserts (equivalent to INSERT IGNORE).
     */
    public static <T> T runIgnoringConstraintViolation(Callable<T> task, T defaultValue)
            throws Exception
    {
        try {
            return task.call();
        }
        catch (UnableToExecuteStatementException e) {
            if (e.getCause() instanceof SQLException) {
                String state = ((SQLException) e.getCause()).getSQLState();
                if (state.startsWith("23")) {
                    return defaultValue;
                }
            }
            throw e;
        }
    }
}
