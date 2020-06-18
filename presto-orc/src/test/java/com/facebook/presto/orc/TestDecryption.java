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

import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.KeyProvider;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.AbstractOrcRecordReader.getDecryptionKeyMetadata;
import static com.facebook.presto.orc.DwrfEncryptionInfo.createNodeToGroupMap;
import static com.facebook.presto.orc.OrcReader.validateEncryption;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.INT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LIST;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.MAP;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestDecryption
{
    private static final List<Slice> A_KEYS = ImmutableList.of(utf8Slice("key1a"), utf8Slice("key2a"));
    private static final List<Slice> B_KEYS = ImmutableList.of(utf8Slice("key1b"), utf8Slice("key2b"));
    private static final StripeInformation A_STRIPE = new StripeInformation(1, 2, 3, 4, 5, A_KEYS);
    private static final StripeInformation NO_KEYS_STRIPE = new StripeInformation(1, 2, 3, 4, 5, ImmutableList.of());
    private static final StripeInformation B_STRIPE = new StripeInformation(1, 2, 3, 4, 5, B_KEYS);

    private static final OrcType ROW_TYPE = new OrcType(LIST, ImmutableList.of(1, 2, 4), ImmutableList.of("col_int", "col_list", "col_map"), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType INT_TYPE = new OrcType(INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType LIST_TYPE = new OrcType(LIST, ImmutableList.of(3), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType MAP_TYPE = new OrcType(MAP, ImmutableList.of(5, 6), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testValidateEncrypted()
    {
        List<EncryptionGroup> encryptionGroups = ImmutableList.of(
                new EncryptionGroup(
                        ImmutableList.of(1, 3),
                        Optional.empty(),
                        Slices.EMPTY_SLICE),
                new EncryptionGroup(
                        ImmutableList.of(4),
                        Optional.empty(),
                        Slices.EMPTY_SLICE));

        Optional<DwrfEncryption> encryption = Optional.of(new DwrfEncryption(KeyProvider.UNKNOWN, encryptionGroups));

        Footer footer = createFooterWithEncryption(ImmutableList.of(A_STRIPE, NO_KEYS_STRIPE), encryption);
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    @Test
    public void testValidateUnencrypted()
    {
        Footer footer = createFooterWithEncryption(ImmutableList.of(NO_KEYS_STRIPE), Optional.empty());
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    @Test(expectedExceptions = OrcCorruptionException.class)
    public void testValidateMissingStripeKeys()
    {
        List<EncryptionGroup> encryptionGroups = ImmutableList.of(
                new EncryptionGroup(
                        ImmutableList.of(1, 3),
                        Optional.empty(),
                        Slices.EMPTY_SLICE),
                new EncryptionGroup(
                        ImmutableList.of(4),
                        Optional.empty(),
                        Slices.EMPTY_SLICE));

        Optional<DwrfEncryption> encryption = Optional.of(new DwrfEncryption(KeyProvider.UNKNOWN, encryptionGroups));
        Footer footer = createFooterWithEncryption(ImmutableList.of(NO_KEYS_STRIPE), encryption);
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    @Test(expectedExceptions = OrcCorruptionException.class)
    public void testValidateMismatchedGroups()
    {
        List<EncryptionGroup> encryptionGroups = ImmutableList.of(
                new EncryptionGroup(
                        ImmutableList.of(1, 3),
                        Optional.empty(),
                        Slices.EMPTY_SLICE));

        Optional<DwrfEncryption> encryption = Optional.of(new DwrfEncryption(KeyProvider.UNKNOWN, encryptionGroups));
        Footer footer = createFooterWithEncryption(ImmutableList.of(A_STRIPE), encryption);
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    private Footer createFooterWithEncryption(List<StripeInformation> stripes, Optional<DwrfEncryption> encryption)
    {
        List<OrcType> types = ImmutableList.of(ROW_TYPE, INT_TYPE, LIST_TYPE, INT_TYPE, MAP_TYPE, INT_TYPE, INT_TYPE);

        return new Footer(
                1,
                2,
                stripes,
                types,
                ImmutableList.of(),
                ImmutableMap.of(),
                encryption);
    }

    @Test
    public void testCreateNodeToGroupMap()
    {
        List<List<Integer>> encryptionGroups = ImmutableList.of(
                ImmutableList.of(1, 3),
                ImmutableList.of(4));

        List<OrcType> types = ImmutableList.of(ROW_TYPE, INT_TYPE, LIST_TYPE, INT_TYPE, MAP_TYPE, INT_TYPE, INT_TYPE);
        Map<Integer, Integer> actual = createNodeToGroupMap(encryptionGroups, types);
        Map<Integer, Integer> expected = ImmutableMap.of(
                1, 0,
                3, 0,
                4, 1,
                5, 1,
                6, 1);
        assertEquals(actual, expected);
    }

    @Test
    public void testGetStripeDecryptionKeys()
    {
        List<StripeInformation> encryptedStripes = ImmutableList.of(A_STRIPE, NO_KEYS_STRIPE, B_STRIPE, NO_KEYS_STRIPE);
        assertEquals(getDecryptionKeyMetadata(0, encryptedStripes), A_KEYS);
        assertEquals(getDecryptionKeyMetadata(1, encryptedStripes), A_KEYS);
        assertEquals(getDecryptionKeyMetadata(2, encryptedStripes), B_KEYS);
        assertEquals(getDecryptionKeyMetadata(3, encryptedStripes), B_KEYS);
    }

    @Test
    public void testGetStripeDecryptionKeysUnencrypted()
    {
        List<StripeInformation> unencryptedStripes = ImmutableList.of(NO_KEYS_STRIPE, NO_KEYS_STRIPE);
        assertEquals(getDecryptionKeyMetadata(0, unencryptedStripes), ImmutableList.of());
        assertEquals(getDecryptionKeyMetadata(1, unencryptedStripes), ImmutableList.of());
    }
}
