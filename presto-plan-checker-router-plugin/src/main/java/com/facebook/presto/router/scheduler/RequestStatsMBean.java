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

package com.facebook.presto.router.scheduler;

public interface RequestStatsMBean
{
    // Java cluster redirect requests
    double getJavaClusterRedirectRequestsOneMinuteCount();

    double getJavaClusterRedirectRequestsOneMinuteRate();

    double getJavaClusterRedirectRequestsFiveMinuteCount();

    double getJavaClusterRedirectRequestsFiveMinuteRate();

    double getJavaClusterRedirectRequestsFifteenMinuteCount();

    double getJavaClusterRedirectRequestsFifteenMinuteRate();

    long getJavaClusterRedirectRequestsTotalCount();

    // Native cluster redirect requests
    double getNativeClusterRedirectRequestsOneMinuteCount();

    double getNativeClusterRedirectRequestsOneMinuteRate();

    double getNativeClusterRedirectRequestsFiveMinuteCount();

    double getNativeClusterRedirectRequestsFiveMinuteRate();

    double getNativeClusterRedirectRequestsFifteenMinuteCount();

    double getNativeClusterRedirectRequestsFifteenMinuteRate();

    long getNativeClusterRedirectRequestsTotalCount();

    // Fallback to Java cluster redirect requests
    double getFallbackToJavaClusterRedirectRequestsOneMinuteCount();

    double getFallbackToJavaClusterRedirectRequestsOneMinuteRate();

    double getFallbackToJavaClusterRedirectRequestsFiveMinuteCount();

    double getFallbackToJavaClusterRedirectRequestsFiveMinuteRate();

    double getFallbackToJavaClusterRedirectRequestsFifteenMinuteCount();

    double getFallbackToJavaClusterRedirectRequestsFifteenMinuteRate();

    long getFallbackToJavaClusterRedirectRequestsTotalCount();
}
