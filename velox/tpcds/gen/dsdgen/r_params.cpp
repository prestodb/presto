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

/*
 * parameter handling functions
 */
#include "velox/tpcds/gen/dsdgen/include/r_params.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <cassert>
#include <cstdint>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#define PARAM_MAX_LEN 80
#define MAX_LINE_LEN 120
#ifdef WIN32
#define OPTION_START '/'
#else
#define OPTION_START '-'
#endif

#ifndef TEST
extern option_t options[];
extern char* params[];
#else
option_t options[] = {
    {"PROG", OPT_STR | OPT_HIDE, 0, NULL, NULL, "tester"},
    {"PARAMS", OPT_STR, 1, "read parameters from file <s>", read_file, ""},
    {"DISTRIBUTIONS",
     OPT_STR,
     2,
     "read distributions from file <s>",
     NULL,
     "tester_dist.idx"},
    {"OUTDIR", OPT_STR, 3, "generate files in directory <s>", NULL, "./"},
    {"VERBOSE", OPT_FLG, 4, "enable verbose output", NULL, "N"},
    {"HELP", OPT_FLG, 5, "display this message", usage, "N"},
    {"scale", OPT_INT, 6, "set scale to <i>", NULL, "1"},
    NULL};
char* params[9];
#endif

int read_file(const char* param_name, const char* option);
int fnd_param(const char* name, DSDGenContext& dsdGenContext);
void print_params(DSDGenContext& dsdGenContext);

/*
 * Routine:  load_params()
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
 * TODO:
 * 20010621 JMS shared memory not yet implemented
 */
void load_params() {
  /*
      int i=0;
      while (options[i].name != NULL)
      {
          load_param(i, GetSharedMemoryParam(options[i].index));
          i++;
      }
      SetSharedMemoryStat(STAT_ROWCOUNT, get_int("STEP"), 0);
  */
  return;
}

/*
 * Routine:  set_flag(int f)
 * Purpose:  set a toggle parameter
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
void set_flg(const char* flag, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(flag, dsdGenContext);
  if (nParam >= 0)
    strcpy(
        dsdGenContext.params[dsdGenContext.options[nParam].index].data(), "Y");

  return;
}

/*
 * Routine: clr_flg(f)
 * Purpose: clear a toggle parameter
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
void clr_flg(const char* flag, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(flag, dsdGenContext);
  if (nParam >= 0)
    strcpy(
        dsdGenContext.params[dsdGenContext.options[nParam].index].data(), "N");
  return;
}

/*
 * Routine: is_set(int f)
 * Purpose: return the state of a toggle parameter, or whether or not a string
 * or int parameter has been set Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int is_set(const char* flag, DSDGenContext& dsdGenContext) {
  int nParam, bIsSet = 0;

  init_params(dsdGenContext);
  nParam = fnd_param(flag, dsdGenContext);
  if (nParam >= 0) {
    if ((static_cast<uint32_t>(dsdGenContext.options[nParam].flags) &
         TYPE_MASK) == OPT_FLG)
      bIsSet =
          (dsdGenContext.params[dsdGenContext.options[nParam].index][0] == 'Y')
          ? 1
          : 0;
    else
      bIsSet = (static_cast<uint32_t>(dsdGenContext.options[nParam].flags) &
                OPT_SET) ||
          (strlen(dsdGenContext.options[nParam].dflt.data()) > 0);
  }

  return (bIsSet); /* better a false negative than a false positive ? */
}

/*
 * Routine: set_int(int var, char *value)
 * Purpose: set an integer parameter
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
void set_int(const char* var, const char* val, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(var, dsdGenContext);
  if (nParam >= 0) {
    strcpy(
        dsdGenContext.params[dsdGenContext.options[nParam].index].data(), val);
    dsdGenContext.options[nParam].flags = static_cast<int>(
        static_cast<uint32_t>(dsdGenContext.options[nParam].flags) | OPT_SET);
  }
  return;
}

/*
 * Routine: get_int(char *var)
 * Purpose: return the value of an integer parameter
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
int get_int(const char* var, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(var, dsdGenContext);
  if (nParam >= 0)
    return (
        atoi(dsdGenContext.params[dsdGenContext.options[nParam].index].data()));
  else
    return (0);
}

double get_dbl(const char* var, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(var, dsdGenContext);
  if (nParam >= 0)
    return (
        atof(dsdGenContext.params[dsdGenContext.options[nParam].index].data()));
  else
    return (0);
}

/*
 * Routine: set_str(int var, char *value)
 * Purpose: set a character parameter
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
void set_str(const char* var, const char* val, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(var, dsdGenContext);
  if (nParam >= 0) {
    strcpy(
        dsdGenContext.params[dsdGenContext.options[nParam].index].data(), val);
    dsdGenContext.options[nParam].flags = static_cast<int>(
        static_cast<uint32_t>(dsdGenContext.options[nParam].flags) | OPT_SET);
  }

  return;
}

/*
 * Routine: get_str(char * var)
 * Purpose: return the value of a character parameter
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
const char* get_str(const char* var, DSDGenContext& dsdGenContext) {
  int nParam;

  init_params(dsdGenContext);
  nParam = fnd_param(var, dsdGenContext);
  if (nParam >= 0)
    return (dsdGenContext.params[dsdGenContext.options[nParam].index].data());
  else
    return (NULL);
}

/*
 * Routine: init_params(DSDGenContext& dsdGenContext)
 * Purpose: initialize a parameter set, setting default values
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
int init_params(DSDGenContext& dsdGenContext) {
  int i;

  if (dsdGenContext.init_params_init)
    return (0);

  for (i = 0; dsdGenContext.options[i].name != NULL; i++) {
    dsdGenContext.params[dsdGenContext.options[i].index].resize(PARAM_MAX_LEN);
    snprintf(
        dsdGenContext.params[dsdGenContext.options[i].index].data(),
        PARAM_MAX_LEN,
        "%s",
        dsdGenContext.options[i].dflt.data());
    if (!dsdGenContext.options[i].dflt.empty())
      dsdGenContext.options[i].flags = static_cast<int>(
          static_cast<uint32_t>(dsdGenContext.options[i].flags) | OPT_DFLT);
  }

  dsdGenContext.init_params_init = 1;

  return (0);
}

/*
 * Routine: save_file(char *path)
 * Purpose: print a summary of options
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
int save_file(const char* path, DSDGenContext& dsdGenContext) {
  int i, w_adjust;
  FILE* ofp;
  time_t timestamp;

  init_params(dsdGenContext);
  time(&timestamp);

  if ((ofp = fopen(path, "w")) == NULL)
    return (-1);

  auto result = fprintf(
      ofp,
      "--\n-- %s Benchmark Parameter File\n-- Created: %s",
      get_str("PROG", dsdGenContext),
      ctime(&timestamp));
  if (result < 0)
    perror("sprintf failed");
  result = fprintf(
      ofp,
      "--\n-- Each entry is of the form: '<parameter> = <value> -- "
      "optional comment'\n");
  if (result < 0)
    perror("sprintf failed");
  result = fprintf(
      ofp, "-- Refer to benchmark documentation for more details\n--\n");
  if (result < 0)
    perror("sprintf failed");

  for (i = 0; dsdGenContext.options[i].name != NULL; i++) {
    if (static_cast<uint32_t>(dsdGenContext.options[i].flags) &
        OPT_HIDE) /* hidden option */
      continue;
    if (strlen(dsdGenContext.params[dsdGenContext.options[i].index].data()) ==
        0)
      continue;

    result = fprintf(ofp, "%s = ", dsdGenContext.options[i].name);
    if (result < 0)
      perror("sprintf failed");
    w_adjust = strlen(dsdGenContext.options[i].name) + 3;
    if (static_cast<uint32_t>(dsdGenContext.options[i].flags) & OPT_STR) {
      result = fprintf(
          ofp,
          "\"%s\"",
          dsdGenContext.params[dsdGenContext.options[i].index].data());
      if (result < 0)
        perror("sprintf failed");
      w_adjust += 2;
    } else {
      result = fprintf(
          ofp,
          "%s",
          dsdGenContext.params[dsdGenContext.options[i].index].data());
      if (result < 0)
        perror("sprintf failed");
    }
    w_adjust +=
        strlen(dsdGenContext.params[dsdGenContext.options[i].index].data()) + 3;
    w_adjust = 60 - w_adjust;
    result =
        fprintf(ofp, "%*s-- %s", w_adjust, " ", dsdGenContext.options[i].usage);
    if (result < 0)
      perror("sprintf failed");
    if (static_cast<uint32_t>(dsdGenContext.options[i].flags) & OPT_NOP) {
      result = fprintf(ofp, " NOT IMPLEMENTED");
      if (result < 0)
        perror("sprintf failed");
    }
    result = fprintf(ofp, "\n");
    if (result < 0)
      perror("sprintf failed");
  }

  fclose(ofp);

  return (0);
}

/*
 * Routine: set_option(int var, char *value)
 * Purpose: set a particular parameter; main entry point for the module
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
int set_option(const char* name, const char* param) {
  printf("ERROR: set_option not supported");
  assert(0);
  exit(1);
}

/*
 * Routine: process_options(int count, char **vector)
 * Purpose:  process a set of command line options
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20000309 need to return integer to allow processing of left-over args
 */
int process_options(
    int count,
    const char** vector,
    DSDGenContext& dsdGenContext) {
  int option_num = 1, res = 1;

  init_params(dsdGenContext);

  while (option_num < count) {
    if (*vector[option_num] == OPTION_START) {
      if (option_num == (count - 1))
        res = set_option(vector[option_num] + 1, NULL);
      else
        res = set_option(vector[option_num] + 1, vector[option_num + 1]);
    }

    if (res < 0) {
      auto result = printf(
          "ERROR: option '%s' or its argument unknown.\n",
          (vector[option_num] + 1));
      if (result < 0)
        perror("sprintf failed");
      usage(NULL, NULL, dsdGenContext);
      exit(1);
    } else
      option_num += res;
  }

#ifdef JMS
  if (is_set("VERBOSE", dsdGenContext))
    print_params(dsdGenContext);
#endif

  return (option_num);
}

/*
 * Routine: print_params(DSDGenContext& dsdGenContext)
 * Purpose: print a parameter summary to display current settings
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
void print_params(DSDGenContext& dsdGenContext) {
  int i;

  init_params(dsdGenContext);

  for (i = 0; dsdGenContext.options[i].name != NULL; i++)
    if (dsdGenContext.options[i].name != NULL) {
      auto result = printf("%s = ", dsdGenContext.options[i].name);
      if (result < 0)
        perror("sprintf failed");
      switch (static_cast<uint32_t>(dsdGenContext.options[i].flags) &
              TYPE_MASK) {
        case OPT_INT: {
          result = printf(
              "%d\n",
              get_int(
                  const_cast<char*>(dsdGenContext.options[i].name),
                  dsdGenContext));
          if (result < 0)
            perror("sprintf failed");
          break;
        }
        case OPT_STR: {
          result = printf(
              "%s\n",
              get_str(
                  const_cast<char*>(dsdGenContext.options[i].name),
                  dsdGenContext));
          if (result < 0)
            perror("sprintf failed");
          break;
        }
        case OPT_FLG: {
          result = printf(
              "%c\n",
              is_set(
                  const_cast<char*>(dsdGenContext.options[i].name),
                  dsdGenContext)
                  ? 'Y'
                  : 'N');
          if (result < 0)
            perror("sprintf failed");
          break;
        }
      }
    }

  return;
}

/*
 * Routine: fnd_param(char *name, int *type, char *value, DSDGenContext&
 * dsdGenContext) Purpose: traverse the defined parameters, looking for a match
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: index of option
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int fnd_param(const char* name, DSDGenContext& dsdGenContext) {
  int i, res = -1;

  for (i = 0; dsdGenContext.options[i].name != NULL; i++) {
    if (strncasecmp(name, dsdGenContext.options[i].name, strlen(name)) == 0) {
      if (res == -1)
        res = i;
      else
        return (-1);
    }
  }

  return (res);
}

/*
 * Routine:  GetParamName(int nParam)
 * Purpose:  Translate between a parameter index and its name
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
char* GetParamName(int nParam, DSDGenContext& dsdGenContext) {
  init_params(dsdGenContext);

  return const_cast<char*>(dsdGenContext.options[nParam].name);
}

/*
 * Routine:  GetParamValue(int nParam)
 * Purpose:  Retrieve a parameters string value based on an index
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
const char* GetParamValue(int nParam, DSDGenContext& dsdGenContext) {
  init_params(dsdGenContext);

  return (dsdGenContext.params[dsdGenContext.options[nParam].index].data());
}

/*
 * Routine:  load_param(char *szValue, int nParam)
 * Purpose:  Set a parameter based on an index
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
int load_param(int nParam, const char* szValue, DSDGenContext& dsdGenContext) {
  init_params(dsdGenContext);

  if (static_cast<uint32_t>(dsdGenContext.options[nParam].flags) &
      OPT_SET) /* already set from the command line */
    return (0);
  else
    strcpy(
        dsdGenContext.params[dsdGenContext.options[nParam].index].data(),
        szValue);

  return (0);
}

/*
 * Routine:  IsIntParam(char *szValue, int nParam)
 * Purpose:  Boolean test for integer parameter
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
int IsIntParam(const char* szParam, DSDGenContext& dsdGenContext) {
  int nParam;

  if ((nParam = fnd_param(szParam, dsdGenContext)) == -1)
    return (nParam);

  return (
      (static_cast<uint32_t>(dsdGenContext.options[nParam].flags) & OPT_INT)
          ? 1
          : 0);
}

/*
 * Routine:  IsStrParam(char *szValue, int nParam)
 * Purpose:  Boolean test for string parameter
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
int IsStrParam(const char* szParam, DSDGenContext& dsdGenContext) {
  int nParam;

  if ((nParam = fnd_param(szParam, dsdGenContext)) == -1)
    return (nParam);

  return (
      (static_cast<uint32_t>(dsdGenContext.options[nParam].flags) & OPT_STR)
          ? 1
          : 0);
}

#ifdef TEST

main() {
  init_params();
  set_int("SCALE", "7");
  set_flg("VERBOSE");
  set_str("DISTRIBUTIONS", "'some file name'");
  print_params();
  set_int("s", "8");
  clr_flg("VERBOSE");
  printf("DIST is %s\n", get_str("DISTRIBUTIONS"));
  print_params();
  usage(NULL, NULL);
}
#endif /* TEST_PARAMS */
