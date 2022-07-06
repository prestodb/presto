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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.StripeInformation;

import java.util.List;
import java.util.Map;

public interface OrcFileIntrospector
{
    default void onFileFooter(Footer fileFooter) {}

    default void onFileTail(OrcFileTail fileTail) {}

    default void onStripe(StripeInformation stripeInformation, Stripe stripe) {}

    default void onRowGroupIndexes(StripeInformation stripe, Map<StreamId, List<RowGroupIndex>> columnIndexes) {}
}
