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
package com.facebook.presto.raptor.storage.organization;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestShardOrganizer
{
    @Test(timeOut = 5_000)
    public void testShardOrganizerInProgress()
            throws Exception
    {
        ShardOrganizer organizer = createShardOrganizer();

        Set<UUID> shards = ImmutableSet.of(UUID.randomUUID());
        OrganizationSet organizationSet = new OrganizationSet(1L, shards, OptionalInt.empty());

        organizer.enqueue(organizationSet);

        assertTrue(organizer.inProgress(getOnlyElement(shards)));
        assertEquals(organizer.getShardsInProgress(), 1);

        while (organizer.inProgress(getOnlyElement(shards))) {
            MILLISECONDS.sleep(10);
        }
        assertFalse(organizer.inProgress(getOnlyElement(shards)));
        assertEquals(organizer.getShardsInProgress(), 0);
        organizer.shutdown();
    }

    private static class MockJobFactory
            implements JobFactory
    {
        @Override
        public Runnable create(OrganizationSet organizationSet)
        {
            return () -> {
                try {
                    MILLISECONDS.sleep(10);
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            };
        }
    }

    static ShardOrganizer createShardOrganizer()
    {
        return new ShardOrganizer(new MockJobFactory(), 1);
    }
}
