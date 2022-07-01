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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertNull;

public class CapturingOrcFileIntrospector
        implements OrcFileIntrospector
{
    private final List<StripeInformation> stripeInformations = new ArrayList<>();
    private final List<Stripe> stripes = new ArrayList<>();
    private final Map<Long, Map<StreamId, List<RowGroupIndex>>> rowGroupIndexesByStripeOffset = new HashMap<>();
    private Footer fileFooter;
    private OrcFileTail fileTail;

    @Override
    public void onFileFooter(Footer fileFooter)
    {
        assertNull(this.fileFooter);
        this.fileFooter = fileFooter;
    }

    @Override
    public void onFileTail(OrcFileTail fileTail)
    {
        assertNull(this.fileTail);
        this.fileTail = fileTail;
    }

    @Override
    public void onStripe(StripeInformation stripeInformation, Stripe stripe)
    {
        this.stripeInformations.add(stripeInformation);
        this.stripes.add(stripe);
    }

    @Override
    public void onRowGroupIndexes(StripeInformation stripe, Map<StreamId, List<RowGroupIndex>> columnIndexes)
    {
        Long stripeOffset = stripe.getOffset();
        assertNull(rowGroupIndexesByStripeOffset.get(stripeOffset));
        rowGroupIndexesByStripeOffset.put(stripeOffset, columnIndexes);
    }

    public List<StripeInformation> getStripeInformations()
    {
        return stripeInformations;
    }

    public List<Stripe> getStripes()
    {
        return stripes;
    }

    public Map<Long, Map<StreamId, List<RowGroupIndex>>> getRowGroupIndexesByStripeOffset()
    {
        return rowGroupIndexesByStripeOffset;
    }

    public Footer getFileFooter()
    {
        return fileFooter;
    }

    public OrcFileTail getFileTail()
    {
        return fileTail;
    }
}
