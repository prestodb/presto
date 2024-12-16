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
 * A copy of the license is included under extension/tpch/dbgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 *//*
 * RANDOM.C -- Implements Park & Miller's "Minimum Standard" RNG
 *
 * (Reference:  CACM, Oct 1988, pp 1192-1201)
 *
 * NextRand:  Computes next random integer
 * UnifInt:   Yields an long uniformly distributed between given bounds
 * UnifReal: ields a real uniformly distributed between given bounds
 * Exponential: Yields a real exponentially distributed with given mean
 *
 */

#include "dbgen/config.h" // @manual

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include "dbgen/dss.h" // @manual
#include "dbgen/rnd.h" // @manual

namespace facebook::velox::tpch::dbgen {

const char* tpch_env_config PROTO((const char* tag, const char* dflt));
void NthElement(DSS_HUGE, DSS_HUGE*);

void dss_random(DSS_HUGE* tgt, DSS_HUGE lower, DSS_HUGE upper, seed_t* seed) {
  *tgt = UnifInt(lower, upper, seed);
  seed->usage += 1;

  return;
}

void row_start(int t, DBGenContext* ctx) {
  int i;
  for (i = 0; i <= MAX_STREAM; i++)
    ctx->Seed[i].usage = 0;

  return;
}

void row_stop_h(int t, DBGenContext* ctx) {
  int i;

  /* need to allow for handling the master and detail together */
  if (t == ORDER_LINE)
    t = ORDER;
  if (t == PART_PSUPP)
    t = PART;

  for (i = 0; i <= MAX_STREAM; i++)
    if ((ctx->Seed[i].table == t) ||
        (ctx->Seed[i].table == ctx->tdefs[t].child)) {
      if (set_seeds && (ctx->Seed[i].usage > ctx->Seed[i].boundary)) {
        fprintf(
            stderr,
            "\nSEED CHANGE: seed[%d].usage = " HUGE_FORMAT "\n",
            i,
            ctx->Seed[i].usage);
        ctx->Seed[i].boundary = ctx->Seed[i].usage;
      } else {
        NthElement(
            (ctx->Seed[i].boundary - ctx->Seed[i].usage), &ctx->Seed[i].value);
#ifdef RNG_TEST
        ctx->Seed[i].nCalls += ctx->Seed[i].boundary - ctx->Seed[i].usage;
#endif
      }
    }
  return;
}

void dump_seeds(int tbl, seed_t* seeds) {
  int i;

  for (i = 0; i <= MAX_STREAM; i++)
    if (seeds[i].table == tbl)
#ifdef RNG_TEST
      printf(
          "%d(" HUGE_FORMAT "):\t" HUGE_FORMAT "\n",
          i,
          seeds[i].nCalls,
          seeds[i].value);
#else
      printf("%d:\t" HUGE_FORMAT "\n", i, seeds[i].value);
#endif
  return;
}

/******************************************************************

   NextRand:  Computes next random integer

*******************************************************************/

/*
 * long NextRand( long nSeed )
 */
DSS_HUGE
NextRand(DSS_HUGE nSeed)

/*
 * nSeed is the previous random number; the returned value is the
 * next random number. The routine generates all numbers in the
 * range 1 .. nM-1.
 */

{
  nSeed = (nSeed * 16807) % 2147483647;
  return (nSeed);
}

/******************************************************************

   UnifInt:  Yields an long uniformly distributed between given bounds

*******************************************************************/

/*
 * long UnifInt( long nLow, long nHigh, seed_t *seed )
 */
DSS_HUGE
UnifInt(DSS_HUGE nLow, DSS_HUGE nHigh, seed_t* seed)

/*
 * Returns an integer uniformly distributed between nLow and nHigh,
 * including * the endpoints. Seed points to the random number stream.
 */

{
  double dRange;
  DSS_HUGE nTemp;
  int32_t nLow32 = static_cast<int32_t>(nLow),
          nHigh32 = static_cast<int32_t>(nHigh);

  if ((nHigh == MAX_LONG) && (nLow == 0)) {
    dRange = static_cast<double>(static_cast<DSS_HUGE>(nHigh32 - nLow32) + 1);
  } else {
    dRange = static_cast<double>(nHigh - nLow + 1);
  }

  seed->value = NextRand(seed->value);
#ifdef RNG_TEST
  seed->nCalls += 1;
#endif
  nTemp = static_cast<DSS_HUGE>(
      (static_cast<double>(seed->value) / DBGenContext::dM) * (dRange));
  return (nLow + nTemp);
}

} // namespace facebook::velox::tpch::dbgen
