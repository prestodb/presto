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
#include <stdlib.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#ifdef WIN32
#include <limits.h>
#include <search.h>
#endif
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/params.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/r_params.h"

#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"

static long Mult = 16807; /* the multiplier */
static long nQ = 127773; /* the quotient MAXINT / Mult */
static long nR = 2836; /* the remainder MAXINT % Mult */
void DSNthElement(HUGE_TYPE N, int nStream, DSDGenContext& dsdGenContext);

/*
 * Routine: next_random(int stream, DSDGenContext& dsdGenContext)
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
long next_random(int stream, DSDGenContext& dsdGenContext) {
  long s = dsdGenContext.Streams[stream].nSeed, div_res, mod_res;

  div_res = s / nQ;
  mod_res = s - nQ * div_res; /* i.e., mod_res = s % nQ */
  s = Mult * mod_res - div_res * nR;
  if (s < 0)
    s += MAXINT;
  dsdGenContext.Streams[stream].nSeed = s;
  dsdGenContext.Streams[stream].nUsed += 1;
#ifdef JMS
  dsdGenContext.Streams[stream].nTotal += 1;
#endif
  return (s);
}

/*
 * Routine: next_random_float(int stream)
 * Purpose:  return random in [0..1]
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
double next_random_float(int stream, DSDGenContext& dsdGenContext) {
  long res;

  res = next_random(stream, dsdGenContext);

  return (static_cast<double>(res) / static_cast<double>(MAXINT));
}

/*
 * Routine: skip_random(int stream, int skip_count, DSDGenContext&
 * dsdGenContext) Purpose: Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void skip_random(int nStream, ds_key_t N, DSDGenContext& dsdGenContext) {
  ds_key_t Z;
  ds_key_t M;

#ifdef UNDEF
  fprintf(stderr, "skipping stream %d to %d\n", nStream, N);
  Streams[nStream].nTotal = N;
#endif
  M = Mult;
  Z = static_cast<ds_key_t>(dsdGenContext.Streams[nStream].nInitialSeed);
  while (N > 0) {
    if (N % 2 != 0) /* testing for oddness, this seems portable */
      Z = (M * Z) % MAXINT;
    N = N / 2; /* integer division, truncates */
    M = (M * M) % MAXINT;
  }
  dsdGenContext.Streams[nStream].nSeed = static_cast<long>(Z);

  return;
}

/*
 * Routine: genrand_integer(int dist, int min, int max, int mean, dsdGenContext)
 * Purpose: generate a random integer given the distribution and limits
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: int
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 */
int genrand_integer(
    int* dest,
    int dist,
    int min,
    int max,
    int mean,
    int stream,
    DSDGenContext& dsdGenContext) {
  int res = 0, i;
  double fres = 0;

  switch (dist) {
    case DIST_UNIFORM:
      res = next_random(stream, dsdGenContext);
      res %= max - min + 1;
      res += min;
      break;
    case DIST_EXPONENTIAL:
      for (i = 0; i < 12; i++)
        fres +=
            static_cast<double>(next_random(stream, dsdGenContext) / MAXINT) -
            0.5;
      res = min + static_cast<int>((max - min + 1) * fres);
      break;
    default:
      INTERNAL("Undefined distribution");
      break;
  }

  if (dest == nullptr)
    return (res);

  *dest = res;

  return (0);
}

/*
 * Routine: genrand_key(ket_t *dest, int dist, ds_key_t min, ds_key_t max,
 * ds_key_t mean, int stream, DSDGenContext& dsdGenContext) Purpose: generate a
 * random integer given the distribution and limits Algorithm: Data Structures:
 *
 * Params:
 * Returns: ds_key_t
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: Need to rework to rely on RNG routines that will work for 64 bit return
 * values
 */
ds_key_t genrand_key(
    ds_key_t* dest,
    int dist,
    ds_key_t min,
    ds_key_t max,
    ds_key_t mean,
    int stream,
    DSDGenContext& dsdGenContext) {
  int res = 0, i;
  double fres = 0;

  switch (dist) {
    case DIST_UNIFORM:
      res = next_random(stream, dsdGenContext);
      res %= static_cast<int>(max - min + 1);
      res += static_cast<int>(min);
      break;
    case DIST_EXPONENTIAL:
      for (i = 0; i < 12; i++)
        fres +=
            static_cast<double>(next_random(stream, dsdGenContext) / MAXINT) -
            0.5;
      res = static_cast<int>(min) + static_cast<int>((max - min + 1) * fres);
      break;
    default:
      INTERNAL("Undefined distribution");
      break;
  }

  if (dest == nullptr)
    return (static_cast<ds_key_t>(res));

  *dest = static_cast<ds_key_t>(res);

  return (static_cast<ds_key_t>(0));
}

/*
 * Routine:
 *	genrand_decimal(int dist, decimal_t *min, decimal_t *max, decimal_t
 **mean), dsdGenContext Purpose: create a random decimal_t Algorithm: Data
 *Structures:
 *
 * Params: min/max are char * to allow easy passing of precision
 * Returns: decimal_t *; NULL on failure
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int genrand_decimal(
    decimal_t* dest,
    int dist,
    decimal_t* min,
    decimal_t* max,
    decimal_t* mean,
    int stream,
    DSDGenContext& dsdGenContext) {
  int i;
  decimal_t res;
  double fres = 0.0;

  if (min->precision < max->precision)
    dest->precision = min->precision;
  else
    dest->precision = max->precision;

  switch (dist) {
    case DIST_UNIFORM:
      res.number = next_random(stream, dsdGenContext);
      res.number %= max->number - min->number + 1;
      res.number += min->number;
      break;
    case DIST_EXPONENTIAL:
      for (i = 0; i < 12; i++) {
        fres /= 2.0;
        fres += static_cast<double>(
                    static_cast<double>(next_random(stream, dsdGenContext)) /
                    static_cast<double>(MAXINT)) -
            0.5;
      }
      res.number = mean->number +
          static_cast<int>((max->number - min->number + 1) * fres);
      break;
    default:
      INTERNAL("Undefined distribution");
      break;
  }

  dest->number = res.number;
  i = 0;
  while (res.number > 10) {
    res.number /= 10;
    i += 1;
  }
  dest->scale = i;

  return (0);
}

/* Routine: RNGReset(int tbl)
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
int RNGReset(int tbl, DSDGenContext& dsdGenContext) {
  int i;

  for (i = 0; dsdGenContext.Streams[i].nColumn != -1; i++)
    if (dsdGenContext.Streams[i].nTable == tbl)
      dsdGenContext.Streams[i].nSeed = dsdGenContext.Streams[i].nInitialSeed;

  return (0);
}

/* WARNING!  This routine assumes the existence of 64-bit                 */

/* integers.  The notation used here- "HUGE" is *not* ANSI standard. */

/* Hopefully, you have this extension as well.  If not, use whatever      */

/* nonstandard trick you need to in order to get 64 bit integers.         */

/* The book says that this will work if MAXINT for the type you choose    */

/* is at least 2**46  - 1, so 64 bits is more than you *really* need      */

static HUGE_TYPE Multiplier = 16807; /* or whatever nonstandard */
static HUGE_TYPE Modulus = 2147483647; /* trick you use to get 64 bit int */

/* Advances value of Seed after N applications of the random number generator
   with multiplier Mult and given Modulus.
   NthElement(Seed[],count);

   Theory:  We are using a generator of the form
        X_n = [Mult * X_(n-1)]  mod Modulus.    It turns out that
        X_n = [(Mult ** n) X_0] mod Modulus.
   This can be computed using a divide-and-conquer technique, see
   the code below.

   In words, this means that if you want the value of the Seed after n
   applications of the generator,  you multiply the initial value of the
   Seed by the "super multiplier" which is the basic multiplier raised
   to the nth power, and then take mod Modulus.
*/

/* Nth Element of sequence starting with StartSeed */
void DSNthElementNthElement(
    HUGE_TYPE N,
    int nStream,
    DSDGenContext& dsdGenContext) {
  HUGE_TYPE Z;
  HUGE_TYPE Mult;

  Mult = Multiplier;
  Z = static_cast<HUGE_TYPE>(dsdGenContext.Streams[nStream].nInitialSeed);
  while (N > 0) {
    if (N % 2 != 0) /* testing for oddness, this seems portable */
    {
#ifdef JMS
      dsdGenContext.Streams[nStream].nTotal += 1;
#endif
      Z = (Mult * Z) % Modulus;
    }
    N = N / 2; /* integer division, truncates */
    Mult = (Mult * Mult) % Modulus;
#ifdef JMS
    dsdGenContext.Streams[nStream].nTotal += 2;
#endif
  }
  dsdGenContext.Streams[nStream].nSeed = static_cast<long>(Z);

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
int dump_seeds_ds(int tbl, DSDGenContext& dsdGenContext) {
  int i;

  for (i = 0; dsdGenContext.Streams[i].nColumn != -1; i++)
    if (dsdGenContext.Streams[i].nTable == tbl) {
      auto result = printf(
          "%04d\t%09d\t%09ld\n",
          i,
          dsdGenContext.Streams[i].nUsed,
          dsdGenContext.Streams[i].nSeed);
      if (result < 0)
        perror("sprintf failed");
    }
  return (0);
}

/*
 * Routine: gen_charset(char *set, int min, int max)
 * Purpose: generate random characters from set for a random length [min..max]
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
int gen_charset(
    char* dest,
    const char* set,
    int min,
    int max,
    int stream,
    DSDGenContext& dsdGenContext) {
  int len, i, temp;

  if (set == NULL) {
    dest = NULL;
    return (-1);
  }

  genrand_integer(&len, DIST_UNIFORM, min, max, 0, stream, dsdGenContext);

  for (i = 0; i < max; i++) {
    genrand_integer(
        &temp, DIST_UNIFORM, 0, strlen(set) - 1, 0, stream, dsdGenContext);
    if (i < len)
      dest[i] = *(set + temp);
  }
  dest[len] = '\0';

  return (0);
}

/*
 * Routine: genrand_date(int dist, date_t *min, date_t *max, DSDGenContext&
 * dsdGenContext) Purpose: generate random date within [min..max] Algorithm:
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
int genrand_date(
    date_t* dest,
    int dist,
    date_t* min,
    date_t* max,
    date_t* mean,
    int stream,
    DSDGenContext& dsdGenContext) {
  int range, imean = 0, temp, idt, nYear, nTotalWeight = 0, nDayCount;

  idt = dttoj(min);
  range = dttoj(max);
  range -= idt;
  nDayCount = min->day;
  nYear = min->year;

  switch (dist) {
    case DIST_SALES:
    case DIST_RETURNS:
      /* walk from min to max to "integrate" the distribution */
      while (range -= 1) {
        nTotalWeight += dist_weight(
            NULL, "calendar", nDayCount, dist + is_leap(nYear), dsdGenContext);
        if (nDayCount == 365 + is_leap(nYear)) {
          nYear += 1;
          nDayCount = 1;
        } else
          nDayCount += 1;
      }
      /* pick a value in the resulting range */
      temp = genrand_integer(
          NULL, DIST_UNIFORM, 1, nTotalWeight, 0, stream, dsdGenContext);
      /* and walk it again to translate that back to a date */
      nDayCount = min->day;
      idt = min->julian;
      nYear = min->year;
      while (temp >= 0) {
        temp -= dist_weight(
            NULL, "calendar", nDayCount, dist + is_leap(nYear), dsdGenContext);
        nDayCount += 1;
        idt += 1;
        if (nDayCount > 365 + is_leap(nYear)) {
          nYear += 1;
          nDayCount = 1;
        }
      }
      break;
    case DIST_EXPONENTIAL:
      imean = dttoj(mean);
      imean -= idt;
    case DIST_UNIFORM:
      genrand_integer(&temp, dist, 0, range, imean, stream, dsdGenContext);
      idt += temp;
      break;
    default:
      break;
  }

  jtodt(dest, idt);

  return (0);
}

/**************
 **************
 **
 ** static routines
 **
 **************
 **************/

/*
 * Routine: init_rand(DSDGenContext& dsdGenContext)
 * Purpose: Initialize the RNG used throughout the code
 * Algorithm: To allow two columns to use the same stream of numbers (for
 *joins), pre-sort the streams list by Duplicate and then assign values. Order
 *by column after initialization Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 */
// FIXME: allow re-init
void init_rand(DSDGenContext& dsdGenContext) {
  long long i, skip, nSeed; // changed to long long from int

  if (!dsdGenContext.init_rand_init) {
    if (is_set("RNGSEED", dsdGenContext))
      nSeed = get_int("RNGSEED", dsdGenContext);
    else
      nSeed = RNG_SEED;
    skip = MAXINT / MAX_COLUMN;
    for (i = 0; i < MAX_COLUMN; i++) {
      // simulate the overflow as if it were an int
      if (i != 0 && (INT_MAX - nSeed) / i < skip) {
        long long val = nSeed + skip * i;
        val %= MAXINT;
        val -= MAXINT;
        val -= 2;
        dsdGenContext.Streams[i].nInitialSeed = val;
        dsdGenContext.Streams[i].nSeed = val;
      } else {
        dsdGenContext.Streams[i].nInitialSeed = nSeed + skip * i;
        dsdGenContext.Streams[i].nSeed = nSeed + skip * i;
      }
      dsdGenContext.Streams[i].nUsed = 0;
    }
    dsdGenContext.init_rand_init = 1;
  }
  return;
}

void resetSeeds(int nTable, DSDGenContext& dsdGenContext) {
  int i;

  for (i = 0; i < MAX_COLUMN; i++)
    if (dsdGenContext.Streams[i].nTable == nTable)
      dsdGenContext.Streams[i].nSeed = dsdGenContext.Streams[i].nInitialSeed;
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
void genrand_email(
    char* pEmail,
    char* pFirst,
    char* pLast,
    int nColumn,
    DSDGenContext& dsdGenContext) {
  char* pDomain = nullptr;
  std::vector<char> szCompany(50);
  int nCompanyLength;

  pick_distribution(&pDomain, "top_domains", 1, 1, nColumn, dsdGenContext);
  genrand_integer(
      &nCompanyLength, DIST_UNIFORM, 10, 20, 0, nColumn, dsdGenContext);
  gen_charset(&szCompany[0], ALPHANUM, 1, 20, nColumn, dsdGenContext);
  szCompany[nCompanyLength] = '\0';

  if ((strlen(pFirst) + strlen(pLast) + szCompany.size() + strlen(pDomain) +
       3) < (RS_C_EMAIL + 1)) {
    auto result = snprintf(
        pEmail,
        RS_C_EMAIL + 1,
        "%s.%s@%s.%s",
        pFirst,
        pLast,
        szCompany.data(),
        pDomain);
    if (result < 0)
      perror("sprintf failed");
  }

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
void genrand_ipaddr(char* pDest, int nColumn, DSDGenContext& dsdGenContext) {
  int arQuads[4], i;

  for (i = 0; i < 4; i++)
    genrand_integer(
        &arQuads[i], DIST_UNIFORM, 1, 255, 0, nColumn, dsdGenContext);
  auto result = snprintf(
      pDest,
      16,
      "%03d.%03d.%03d.%03d",
      arQuads[0],
      arQuads[1],
      arQuads[2],
      arQuads[3]);
  if (result < 0)
    perror("sprintf failed");

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
int genrand_url(char* pDest, int nColumn) {
  strcpy(pDest, "http://www.foo.com");

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
int setSeed(int nStream, int nValue, DSDGenContext& dsdGenContext) {
  int nRetValue;

  nRetValue = dsdGenContext.Streams[nStream].nSeed;
  dsdGenContext.Streams[nStream].nSeed = nValue;

  return (nRetValue);
}

#ifdef TEST
main() {
  printf("r_genrand:No test routine has been defined for this module\n");

  exit(0);
}
#endif /* TEST */
