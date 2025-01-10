/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

int* makePermutation(
    int* pNumberSet,
    int nSize,
    int nStream,
    DSDGenContext& dsdGenContext);
ds_key_t* makeKeyPermutation(
    ds_key_t* pNumberSet,
    ds_key_t nSize,
    int nStream,
    DSDGenContext& dsdGenContext);
#define getPermutationEntry(pPermutation, nIndex) (pPermutation[nIndex - 1] + 1)
