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
package com.facebook.presto.hive.authentication;

import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;

final class UserGroupInformationUtils
{
    private UserGroupInformationUtils() {}

    static <R, E extends Exception> R executeActionInDoAs(UserGroupInformation userGroupInformation, GenericExceptionAction<R, E> action)
            throws E
    {
        return userGroupInformation.doAs((PrivilegedAction<ResultOrException<R, E>>) () -> {
            try {
                return new ResultOrException<>(action.run(), null);
            }
            catch (Throwable e) {
                return new ResultOrException<>(null, e);
            }
        }).get();
    }

    private static class ResultOrException<T, E extends Exception>
    {
        private final T result;
        private final Throwable exception;

        public ResultOrException(T result, Throwable exception)
        {
            this.result = result;
            this.exception = exception;
        }

        @SuppressWarnings("unchecked")
        public T get()
                throws E
        {
            if (exception != null) {
                if (exception instanceof Error) {
                    throw (Error) exception;
                }
                if (exception instanceof RuntimeException) {
                    throw (RuntimeException) exception;
                }
                throw (E) exception;
            }
            return result;
        }
    }
}
