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

#ifndef BUILD_SUPPORT_H
#define BUILD_SUPPORT_H

#include "columns.h"
#include "date.h"
#include "decimal.h"
#include "dist.h"
#include "pricing.h"

void bitmap_to_dist(
    void* pDest,
    const char* distname,
    ds_key_t* modulus,
    int vset,
    int stream,
    DSDGenContext& dsdGenContext);
void dist_to_bitmap(
    int* pDest,
    const char* szDistName,
    int nValue,
    int nWeight,
    int nStream);
void random_to_bitmap(
    int* pDest,
    int nDist,
    int nMin,
    int nMax,
    int nMean,
    int nStream);
int city_hash(int nTable, char* city);
void hierarchy_item(
    int h_level,
    ds_key_t* id,
    char** name,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
ds_key_t mk_join(
    int from_tbl,
    int to_tbl,
    ds_key_t ref_key,
    DSDGenContext& dsdGenContext);
ds_key_t getCatalogNumberFromPage(ds_key_t kPageNumber);
void mk_word(
    char* dest,
    const char* syl_set,
    ds_key_t src,
    int char_cnt,
    int col,
    DSDGenContext& dsdGenContext);
int set_locale(int nRegion, decimal_t* longitude, decimal_t* latitude);
int adj_time(
    ds_key_t* res_date,
    ds_key_t* res_time,
    ds_key_t base_date,
    ds_key_t base_time,
    ds_key_t offset_key,
    int tabid);
void mk_bkey(char* szDest, ds_key_t kPrimary, int nStream);
int embed_string(
    char* szDest,
    const char* szDist,
    int nValue,
    int nWeight,
    int nStream,
    DSDGenContext& dsdGenContext);
int mk_companyname(
    char* dest,
    int nTable,
    int nCompany,
    DSDGenContext& dsdGenContext);
void setUpdateDateRange(
    int nTable,
    date_t* pMinDate,
    date_t* pMaxDate,
    DSDGenContext& dsdGenContext);

#endif /* BUILD_SUPPORT_H */
