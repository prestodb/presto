/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#include "velox/tpcds/gen/dsdgen/include/address.h"
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/permute.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/r_params.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

static int s_nCountyCount = 0;
static int s_nCityCount = 0;

void resetCountCount(void) {
  s_nCountyCount = 0;
  s_nCityCount = 0;

  return;
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int mk_address(ds_addr_t* pAddr, int nColumn, DSDGenContext& dsdGenContext) {
  int i = 0, nRegion = 0;
  char *szZipPrefix = nullptr, szAddr[100];
  int nMaxCities, nMaxCounties;
  tdef* pTdef;

  nMaxCities = static_cast<int>(get_rowcount(ACTIVE_CITIES, dsdGenContext));
  nMaxCounties = static_cast<int>(get_rowcount(ACTIVE_COUNTIES, dsdGenContext));

  /* street_number is [1..1000] */
  genrand_integer(
      &pAddr->street_num, DIST_UNIFORM, 1, 1000, 0, nColumn, dsdGenContext);

  /* street names are picked from a distribution */
  pick_distribution(
      &pAddr->street_name1, "street_names", 1, 1, nColumn, dsdGenContext);
  pick_distribution(
      &pAddr->street_name2, "street_names", 1, 2, nColumn, dsdGenContext);

  /* street type is picked from a distribution */
  pick_distribution(
      &pAddr->street_type, "street_type", 1, 1, nColumn, dsdGenContext);

  /* suite number is alphabetic 50% of the time */
  genrand_integer(&i, DIST_UNIFORM, 1, 100, 0, nColumn, dsdGenContext);
  if (i & 0x01) {
    auto result = snprintf(
        pAddr->suite_num, sizeof(pAddr->suite_num), "Suite %d", (i >> 1) * 10);
    if (result < 0)
      perror("sprintf failed");
  } else {
    auto result = snprintf(
        pAddr->suite_num,
        sizeof(pAddr->suite_num),
        "Suite %c",
        ((i >> 1) % 25) + 'A');
    if (result < 0)
      perror("sprintf failed");
  }

  pTdef = getTdefsByNumber(
      getTableFromColumn(nColumn, dsdGenContext), dsdGenContext);

  /* city is picked from a distribution which maps to large/medium/small */
  if (pTdef->flags & FL_SMALL) {
    i = static_cast<int>(get_rowcount(
        getTableFromColumn(nColumn, dsdGenContext), dsdGenContext));
    genrand_integer(
        &i,
        DIST_UNIFORM,
        1,
        (nMaxCities > i) ? i : nMaxCities,
        0,
        nColumn,
        dsdGenContext);
    dist_member(&pAddr->city, "cities", i, 1, dsdGenContext);
  } else
    pick_distribution(&pAddr->city, "cities", 1, 6, nColumn, dsdGenContext);

  /* county is picked from a distribution, based on population and keys the
   * rest */
  if (pTdef->flags & FL_SMALL) {
    i = (int)get_rowcount(
        getTableFromColumn(nColumn, dsdGenContext), dsdGenContext);
    genrand_integer(
        &nRegion,
        DIST_UNIFORM,
        1,
        (nMaxCounties > i) ? i : nMaxCounties,
        0,
        nColumn,
        dsdGenContext);
    dist_member(&pAddr->county, "fips_county", nRegion, 2, dsdGenContext);
  } else
    nRegion = pick_distribution(
        &pAddr->county, "fips_county", 2, 1, nColumn, dsdGenContext);

  /* match state with the selected region/county */
  dist_member(&pAddr->state, "fips_county", nRegion, 3, dsdGenContext);

  /* match the zip prefix with the selected region/county */
  pAddr->zip = city_hash(0, pAddr->city);
  /* 00000 - 00600 are unused. Avoid them */
  dist_member(
      static_cast<void*>(&szZipPrefix),
      "fips_county",
      nRegion,
      5,
      dsdGenContext);
  if (!(szZipPrefix[0] - '0') && (pAddr->zip < 9400))
    pAddr->zip += 600;
  pAddr->zip += (szZipPrefix[0] - '0') * 10000;

  auto result = snprintf(
      szAddr,
      sizeof(szAddr),
      "%d %s %s %s",
      pAddr->street_num,
      pAddr->street_name1,
      pAddr->street_name2,
      pAddr->street_type);
  if (result < 0)
    perror("sprintf failed");
  pAddr->plus4 = city_hash(0, szAddr);
  dist_member(&pAddr->gmt_offset, "fips_county", nRegion, 6, dsdGenContext);
  strcpy(pAddr->country, "United States");

  return (0);
}

/*
 * Routine: mk_streetnumber
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the random number Returns: Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20030422 jms should be replaced if there is no table variation
 */
int mk_streetnumber(int nTable, int* dest, DSDGenContext& dsdGenContext) {
  genrand_integer(dest, DIST_UNIFORM, 1, 1000, 0, nTable, dsdGenContext);

  return (0);
}

/*
 * Routine: mk_suitenumber()
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the random number Returns: Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20010615 JMS return code is meaningless
 */
int mk_suitenumber(int nTable, char* dest, DSDGenContext& dsdGenContext) {
  int i;

  genrand_integer(&i, DIST_UNIFORM, 1, 100, 0, nTable, dsdGenContext);
  if (i <= 50) {
    genrand_integer(&i, DIST_UNIFORM, 1, 1000, 0, nTable, dsdGenContext);
    auto result = snprintf(dest, 11, "Suite %d", i);
    if (result < 0)
      perror("sprintf failed");
  } else {
    genrand_integer(&i, DIST_UNIFORM, 0, 25, 0, nTable, dsdGenContext);
    auto result = snprintf(dest, 10, "Suite %c", i + 'A');
    if (result < 0)
      perror("sprintf failed");
  }

  return (0);
}

/*
 * Routine: mk_streetname()
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 *	use a staggered distibution and the 150 most common street names in the
 *US Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the street name Returns: Called By: Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20010615 JMS return code is meaningless
 */
int mk_streetname(int nTable, char* dest, DSDGenContext& dsdGenContext) {
  char *pTemp1 = nullptr, *pTemp2 = nullptr;

  pick_distribution(
      static_cast<void*>(&pTemp1),
      "street_names",
      (int)1,
      (int)1,
      nTable,
      dsdGenContext);
  pick_distribution(
      static_cast<void*>(&pTemp2),
      "street_names",
      (int)1,
      (int)2,
      nTable,
      dsdGenContext);

  if (pTemp1 && pTemp2) {
    if (strlen(pTemp2)) {
      auto result = snprintf(
          dest, strlen(pTemp1) + strlen(pTemp2) + 2, "%s %s", pTemp1, pTemp2);
      if (result < 0)
        perror("sprintf failed");
    } else
      strcpy(dest, pTemp1);
  }

  return (0);
}

/*
 * Routine: mk_city
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 *	use a staggered distibution of 1000 most common place names in the US
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the city name Returns: Called By: Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20030423 jms should be replaced if there is no per-table variation
 */
int mk_city(int nTable, char** dest, DSDGenContext& dsdGenContext) {
  pick_distribution(
      static_cast<void*>(dest),
      "cities",
      static_cast<int>(1),
      static_cast<int>(get_int("_SCALE_INDEX", dsdGenContext)),
      11,
      dsdGenContext);

  return (0);
}

/*
 * Routine: city_hash()
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int city_hash(int nTable, char* name) {
  char* cp;
  uint64_t hash_value = 0, res = 0;

  for (cp = name; *cp; cp++) {
    hash_value *= 26;
    hash_value -= 'A';
    hash_value += *cp;
    if (hash_value > 1000000) {
      hash_value %= 10000;
      res += hash_value;
      hash_value = 0;
    }
  }
  hash_value %= 1000;
  res += hash_value;
  res %= 10000; /* looking for a 4 digit result */

  return (res);
}

/*
 * Routine:
 *	one of a set of routines that creates addresses
 * Algorithm:
 *	use a compound distribution of the 3500 counties in the US
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the city name nRegion: the county selected
 *	city: the city name selected
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20010615 JMS return code is meaningless
 */
int mk_zipcode(
    int nTable,
    char* dest,
    int nRegion,
    char* city,
    DSDGenContext& dsdGenContext) {
  char* szZipPrefix = nullptr;
  int nCityCode;
  int nPlusFour;

  if (szZipPrefix) {
    dist_member(
        static_cast<void*>(&szZipPrefix),
        "fips_county",
        nRegion,
        5,
        dsdGenContext);
    nCityCode = city_hash(nTable, city);
    genrand_integer(
        &nPlusFour, DIST_UNIFORM, 1, 9999, 0, nTable, dsdGenContext);
    auto result = snprintf(
        dest,
        strlen(szZipPrefix) + 10,
        "%s%04d-%04d",
        szZipPrefix,
        nCityCode,
        nPlusFour);
    if (result < 0)
      perror("sprintf failed");
  }

  return (0);
}
