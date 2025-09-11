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

#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#ifndef WIN32
#include <netinet/in.h>
#endif
#include <math.h>
#include "velox/tpcds/gen/dsdgen/include/StringBuffer.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/error_msg.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/r_params.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

/*
 * Routine: hierarchy_item
 * Purpose:
 *	select the hierarchy entry for this level
 * Algorithm: Assumes a top-down ordering
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 */
void hierarchy_item(
    int h_level,
    ds_key_t* id,
    char** name,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext) {
  static int nLastCategory = -1, nLastClass = -1, nBrandBase;
  int nBrandCount = 0;
  static char* szClassDistName = nullptr;

  switch (h_level) {
    case I_CATEGORY:
      nLastCategory =
          pick_distribution(name, "categories", 1, 1, h_level, dsdGenContext);
      *id = nLastCategory;
      nBrandBase = nLastCategory;
      nLastClass = -1;
      break;
    case I_CLASS:
      if (nLastCategory == -1)
        ReportErrorNoLine(
            DBGEN_ERROR_HIERACHY_ORDER, "I_CLASS before I_CATEGORY", 1);
      dist_member(
          &szClassDistName, "categories", nLastCategory, 2, dsdGenContext);
      nLastClass = pick_distribution(
          name, szClassDistName, 1, 1, h_level, dsdGenContext);
      nLastCategory = -1;
      *id = nLastClass;
      break;
    case I_BRAND: {
      if (nLastClass == -1)
        ReportErrorNoLine(
            DBGEN_ERROR_HIERACHY_ORDER, "I_BRAND before I_CLASS", 1);
      dist_member(&nBrandCount, szClassDistName, nLastClass, 2, dsdGenContext);
      *id = kIndex % nBrandCount + 1;
      mk_word(
          *name,
          "brand_syllables",
          nBrandBase * 10 + nLastClass,
          45,
          I_BRAND,
          dsdGenContext);
      int idValue = static_cast<int>(*id);
      std::vector<char> sTemp(6 + std::to_string(idValue).size());
      auto result = snprintf(sTemp.data(), sTemp.size(), " #%d", idValue);
      if (result < 0)
        perror("sprintf failed");
      strcat(*name, sTemp.data());
      *id += (nBrandBase * 1000 + nLastClass) * 1000;
      break;
    }
    default:
      auto result = printf(
          "ERROR: Invalid call to hierarchy_item with argument '%d'\n",
          h_level);
      if (result < 0)
        perror("sprintf failed");
      exit(1);
  }

  return;
}

/*
 * Routine: mk_companyname()
 * Purpose:
 *	yet another member of a set of routines used for address creation
 * Algorithm:
 *	create a hash, based on an index value, so that the same result can be
 *derived reliably and then build a word from a syllable set Data Structures:
 *
 * Params:
 *	char * dest: target for resulting name
 *	int nTable: to allow differing distributions
 *	int nCompany: index value
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 *	20010615 JMS return code is meaningless
 *	20030422 JMS should be replaced if there is no per-table variation
 */
int mk_companyname(
    char* dest,
    int nTable,
    int nCompany,
    DSDGenContext& dsdGenContext) {
  mk_word(dest, "syllables", nCompany, 10, CC_COMPANY_NAME, dsdGenContext);

  return (0);
}

/*
 * Routine: set_locale()
 * Purpose:
 *	generate a reasonable lattitude and longitude based on a region and the
 *USGS data on 3500 counties in the US Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20011230 JMS set_locale() is just a placeholder; do we need geographic
 *coords?
 */
int set_locale(int nRegion, decimal_t* longitude, decimal_t* latitude) {
  static int init = 0;
  static decimal_t dZero;

  if (!init) {
    strtodec(&dZero, "0.00");
    init = 1;
  }

  memcpy(longitude, &dZero, sizeof(decimal_t));
  memcpy(latitude, &dZero, sizeof(decimal_t));

  return (0);
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
void bitmap_to_dist(
    void* pDest,
    const char* distname,
    ds_key_t* modulus,
    int vset,
    int stream,
    DSDGenContext& dsdGenContext) {
  int32_t m, s;
  unsigned int len = strlen(distname);
  std::vector<char> msg(len + 31);

  if ((s = distsize(distname, dsdGenContext)) == -1) {
    auto result = snprintf(
        msg.data(), msg.size(), "Invalid distribution name '%s'", distname);
    if (result < 0)
      perror("sprintf failed");
    INTERNAL(msg.data());
  }
  m = static_cast<int32_t>((*modulus % s) + 1);
  *modulus /= s;

  dist_member(pDest, distname, m, vset, dsdGenContext);

  return;
}

/*
 * Routine: void dist_to_bitmap(int *pDest, char *szDistName, int nValueSet, int
 * nWeightSet, int nStream) Purpose: Reverse engineer a composite key based on
 * distributions Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void dist_to_bitmap(
    int* pDest,
    const char* szDistName,
    int nValue,
    int nWeight,
    int nStream,
    DSDGenContext& dsdGenContext) {
  *pDest *= distsize(szDistName, dsdGenContext);
  *pDest += pick_distribution(
      NULL, szDistName, nValue, nWeight, nStream, dsdGenContext);

  return;
}

/*
 * Routine: void random_to_bitmap(int *pDest, int nDist, int nMin, int nMax, int
 * nMean, int nStream) Purpose: Reverse engineer a composite key based on an
 * integer range Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void random_to_bitmap(
    int* pDest,
    int nDist,
    int nMin,
    int nMax,
    int nMean,
    int nStream,
    DSDGenContext& dsdGenContext) {
  *pDest *= nMax;
  *pDest +=
      genrand_integer(NULL, nDist, nMin, nMax, nMean, nStream, dsdGenContext);

  return;
}

/*
 * Routine: mk_word()
 * Purpose:
 *	generate a gibberish word from a given syllable set
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 */
void mk_word(
    char* dest,
    const char* syl_set,
    ds_key_t src,
    int char_cnt,
    int col,
    DSDGenContext& dsdGenContext) {
  ds_key_t i = src, nSyllableCount;
  char* cp = nullptr;

  *dest = '\0';
  while (i > 0) {
    nSyllableCount = distsize(syl_set, dsdGenContext);
    dist_member(
        &cp,
        syl_set,
        static_cast<int>(i % nSyllableCount) + 1,
        1,
        dsdGenContext);
    i /= nSyllableCount;
    if (static_cast<int>(strlen(dest) + strlen(cp)) <= char_cnt)
      strcat(dest, cp);
    else
      break;
  }

  return;
}

/*
 * Routine: mk_surrogate()
 * Purpose: create a character based surrogate key from a 64-bit value
 * Algorithm: since the RNG routines produce a 32bit value, and surrogate keys
 *can reach beyond that, use the RNG output to generate the lower end of a
 *random string, and build the upper end from a ds_key_t Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls: ltoc()
 * Assumptions: output is a 16 character string. Space is not checked
 * Side Effects:
 * TODO:
 * 20020830 jms may need to define a 64-bit form of htonl() for portable shift
 *operations
 */
void ltoc(char* szDest, unsigned long nVal) {
  char szXlate[16] = {
      'A',
      'B',
      'C',
      'D',
      'E',
      'F',
      'G',
      'H',
      'I',
      'J',
      'K',
      'L',
      'M',
      'N',
      'O',
      'P'};

  char c;
  for (int i = 0; i < 8; i++) {
    c = szXlate[(static_cast<uint32_t>(nVal) & 0xF)];
    *szDest++ = c;
    nVal >>= 4;
  }
  *szDest = '\0';
}

void mk_bkey(char* szDest, ds_key_t kPrimary, int nStream) {
  unsigned long nTemp;

  nTemp = static_cast<unsigned long>(kPrimary >> 32);
  ltoc(szDest, nTemp);

  nTemp =
      static_cast<unsigned long>(static_cast<uint32_t>(kPrimary) & 0xFFFFFFFF);
  ltoc(szDest + 8, nTemp);

  return;
}

/*
 * Routine: embed_string(char *szDest, char *szDist, int nValue, int nWeight,
 * int nStream, DSDGenContext& dsdGenContext) Purpose: Algorithm: Data
 * Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int embed_string(
    char* szDest,
    const char* szDist,
    int nValue,
    int nWeight,
    int nStream,
    DSDGenContext& dsdGenContext) {
  int nPosition = 0;
  char* szWord = nullptr;

  pick_distribution(&szWord, szDist, nValue, nWeight, nStream, dsdGenContext);
  nPosition = genrand_integer(
      NULL,
      DIST_UNIFORM,
      0,
      strlen(szDest) - strlen(szWord) - 1,
      0,
      nStream,
      dsdGenContext);
  memcpy(&szDest[nPosition], szWord, sizeof(char) * strlen(szWord));

  return (0);
}

/*
 * Routine: adjust the valid date window for source schema tables, based on
 *	based on the update count, update window size, etc.
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
void setUpdateDateRange(
    int nTable,
    date_t* pMinDate,
    date_t* pMaxDate,
    DSDGenContext& dsdGenContext) {
  static int nUpdateNumber;

  if (!dsdGenContext.setUpdateDateRange_init) {
    nUpdateNumber = get_int("UPDATE", dsdGenContext);
    dsdGenContext.setUpdateDateRange_init = 1;
  }

  switch (nTable) /* no per-table changes at the moment; but could be */
  {
    default:
      strtodt(pMinDate, WAREHOUSE_LOAD_DATE);
      pMinDate->julian += UPDATE_INTERVAL * (nUpdateNumber - 1);
      jtodt(pMinDate, pMinDate->julian);
      jtodt(pMaxDate, pMinDate->julian + UPDATE_INTERVAL);
      break;
  }

  return;
}
