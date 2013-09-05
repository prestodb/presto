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
package com.facebook.presto.client;

public final class PrestoHeaders
{
    public static final String PRESTO_USER = "X-Presto-User";
    public static final String PRESTO_SOURCE = "X-Presto-Source";
    public static final String PRESTO_CATALOG = "X-Presto-Catalog";
    public static final String PRESTO_SCHEMA = "X-Presto-Schema";

    public static final String PRESTO_CURRENT_STATE = "X-Presto-Current-State";
    public static final String PRESTO_MAX_WAIT = "X-Presto-Max-Wait";
    public static final String PRESTO_MAX_SIZE = "X-Presto-Max-Size";
    public static final String PRESTO_PAGE_TOKEN = "X-Presto-Page-Sequence-Id";
    public static final String PRESTO_PAGE_NEXT_TOKEN = "X-Presto-Page-End-Sequence-Id";

    public PrestoHeaders()
    {
    }
}
