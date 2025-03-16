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

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <time.h>
#include <cstdint>
#include "config.h"
#include "porting.h"
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include <fcntl.h>
#ifdef AIX
#include <sys/mode.h>
#endif /* AIX */
#include <sys/stat.h>
#include <sys/types.h>
#include "date.h"
#include "decimal.h"
#include "dist.h"
#include "genrand.h"
#include "misc.h"
#include "r_params.h"
#include "tdefs.h"

static char* alpha_num =
    "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

char* getenv(const char* name);
int print_separator(int sep);

extern long Seed[];

#ifdef WIN32
#define PATH_SEP '\\'
#else
#define PATH_SEP '/'
#endif

int file_num = -1;

/*
 *
 * Various routines that handle distributions, value selections and
 * seed value management for the DSS benchmark. Current functions:
 * env_config -- set config vars with optional environment override
 * a_rnd(min, max) -- random alphanumeric within length range
 */

/*
 * env_config: look for a environmental variable setting and return its
 * value; otherwise return the default supplied
 */
char* env_config(char* var, char* dflt) {
  static char* evar;

  if ((evar = getenv(var)) != NULL)
    return (evar);
  else
    return (dflt);
}

/*
 * generate a random string with length randomly selected in [min, max]
 * and using the characters in alphanum (currently includes a space
 * and comma)
 */
int a_rnd(
    int min,
    int max,
    int column,
    char* dest,
    DSDGenContext& dsdGenContext) {
  int i, len, char_int;

  genrand_integer(&len, DIST_UNIFORM, min, max, 0, column, dsdGenContext);
  for (i = 0; i < len; i++) {
    if (i % 5 == 0)
      genrand_integer(
          &char_int,
          DIST_UNIFORM,
          0,
          static_cast<uint32_t>(1) << 30,
          0,
          column,
          dsdGenContext);
    *(dest + i) = alpha_num[static_cast<uint32_t>(char_int) & 077];
    char_int = static_cast<int>(static_cast<uint32_t>(char_int) >> 6);
  }
  *(dest + len) = '\0';
  return (len);
}
