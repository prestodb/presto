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

#include "config.h"
#include "porting.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>

#define MAX_LINE_LEN 120
#ifdef WIN32
#define OPTION_START '/'
#else
#define OPTION_START '-'
#endif
#ifdef _WIN32
#include <io.h>
#include <search.h>
#include <stdlib.h>
#include <winsock.h>
#else
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif
#ifdef NCR
#include <sys/types.h>
#endif
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "config.h"
#include "date.h"
#include "dcomp.h"
#include "decimal.h"
#include "dist.h"
#include "error_msg.h"
#include "genrand.h"
#include "r_params.h"
#include "tpcds_idx.hpp"
#ifdef TEST
option_t options[] = {
    {"DISTRIBUTIONS",
     OPT_STR,
     2,
     "read distributions from file <s>",
     NULL,
     "tester_dist.idx"},
    NULL};

char params[2];
struct {
  char* name;
} tdefs[] = {NULL};
#endif

/* NOTE: these need to be in sync with a_dist.h */
#define D_NAME_LEN 20
#define FL_LOADED 0x01
static int load_dist(d_idx_t* d);

#include <stdio.h>
#include "config.h"
#include "porting.h"
#include "r_params.h"

/*
 * Routine: release(char *param_name, char *msg)
 * Purpose: display version information
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
int printReleaseInfo(
    const char* param_name,
    const char* msg,
    DSDGenContext& dsdGenContext) {
  auto result = fprintf(
      stderr,
      "%s Population Generator (Version %d.%d.%d%s)\n",
      get_str("PROG", dsdGenContext),
      VERSION,
      RELEASE,
      MODIFICATION,
      PATCH);
  if (result < 0)
    perror("sprintf failed");
  result = fprintf(stderr, "Copyright %s %s\n", COPYRIGHT, C_DATES);
  if (result < 0)
    perror("sprintf failed");

  exit(0);
}

/*
 * Routine: usage(char *param_name, char *msg)
 * Purpose: display a usage message, with an optional error message
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
int usage(
    const char* param_name,
    const char* msg,
    DSDGenContext& dsdGenContext) {
  init_params(dsdGenContext);

  auto result = fprintf(
      stderr,
      "%s Population Generator (Version %d.%d.%d%s)\n",
      get_str("PROG", dsdGenContext),
      VERSION,
      RELEASE,
      MODIFICATION,
      PATCH);
  if (result < 0)
    perror("sprintf failed");
  result = fprintf(stderr, "Copyright %s %s\n", COPYRIGHT, C_DATES);
  if (result < 0)
    perror("sprintf failed");

  if (msg != NULL)
    printf("\nERROR: %s\n\n", msg);

  printf("\n\nUSAGE: %s [options]\n", get_str("PROG", dsdGenContext));
  printf(
      "\nNote: When defined in a parameter file (using -p), parmeters "
      "should\n");
  printf("use the form below. Each option can also be set from the command\n");
  printf("line, using a form of '%cparam [optional argument]'\n", OPTION_START);
  printf("Unique anchored substrings of options are also recognized, and \n");
  printf(
      "case is ignored, so '%csc' is equivalent to '%cSCALE'\n\n",
      OPTION_START,
      OPTION_START);
  printf("General Options\n===============\n");
  print_options(dsdGenContext.options, 0, dsdGenContext);
  printf("\n");
  printf("Advanced Options\n===============\n");
  print_options(dsdGenContext.options, 1, dsdGenContext);
  printf("\n");
  exit((msg == NULL) ? 0 : 1);
}

/*
 * Routine: read_file(char *param_name, char *fname)
 * Purpose: process a parameter file
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
int read_file(
    const char* param_name,
    const char* optarg,
    DSDGenContext& dsdGenContext) {
  FILE* fp = nullptr;
  char* cp = nullptr;
  char line[MAX_LINE_LEN];
  char name[100];
  int index = 0;

  init_params(dsdGenContext);

  if ((fp = fopen(optarg, "r")) == NULL)
    return (-1);
  while (fgets(line, MAX_LINE_LEN, fp) != NULL) {
    if ((cp = strchr(line, '\n')) != NULL)
      *cp = '\0';
    if ((cp = strchr(line, '-')) != NULL)
      if (*(cp + 1) == '-')
        *cp = '\0';
    if ((cp = strtok(line, " \t=\n")) != NULL) {
      strcpy(name, cp);
      index = fnd_param(name, dsdGenContext);
      if (index == -1)
        continue; /* JMS: errors are silently ignored */
      cp += strlen(cp) + 1;
      while (*cp && strchr(" \t =", *cp))
        cp++;

      /* command line options over-ride those in a file */
      if (static_cast<unsigned int>(dsdGenContext.options[index].flags) &
          OPT_SET)
        continue;

      if (*cp) {
        switch (static_cast<unsigned int>(dsdGenContext.options[index].flags) &
                TYPE_MASK) {
          case OPT_INT:
            if ((cp = strtok(cp, " \t\n")) != NULL)
              set_option(name, cp);
            break;
          case OPT_STR:
          case OPT_FLG:
            set_option(name, cp);
            break;
        }
      }
    }
  }

  fclose(fp);

  return (0);
}

/*
 * Routine: set_scale()
 * Purpose: link SCALE and SCALE_INDEX
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
int SetScaleIndex(
    const char* szName,
    const char* szValue,
    DSDGenContext& dsdGenContext) {
  char szScale[2];
  int nScale;

  if ((nScale = atoi(szValue)) == 0)
    nScale = 1;

  nScale = 1 + static_cast<int>(log10(nScale));
  szScale[0] = '0' + nScale;
  szScale[1] = '\0';

  set_int("_SCALE_INDEX", szScale, dsdGenContext);

  return (atoi(szValue));
}

/*
 * Routine: print_options(struct OPTION_T *o, int file, int depth)
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
static void print_options(
    struct OPTION_T* o,
    int bShowOptional,
    DSDGenContext& dsdGenContext) {
  int w_adjust = 0, bShow = 0, nCount = 0;

  for (int i = 0; dsdGenContext.options[i].name != NULL; i++) {
    /*
     * options come in two groups, general and "hidden". Decide which group
     * to show in this pass, and ignore others
     */
    bShow = 0;
    if (bShowOptional && (static_cast<unsigned int>(o[i].flags) & OPT_ADV))
      bShow = 1;
    if (!bShowOptional && !(static_cast<unsigned int>(o[i].flags) & OPT_ADV))
      bShow = 1;

    if (!bShow || (static_cast<unsigned int>(o[i].flags) & OPT_HIDE))
      continue;

    nCount += 1;
    printf("%s = ", o[i].name);
    w_adjust = 15 - strlen(o[i].name);
    if (static_cast<unsigned int>(o[i].flags) & OPT_INT)
      printf(" <n>   ");
    else if (static_cast<unsigned int>(o[i].flags) & OPT_STR)
      printf(" <s>   ");
    else if (static_cast<unsigned int>(o[i].flags) & OPT_SUB)
      printf(" <opt> ");
    else if (static_cast<unsigned int>(o[i].flags) & OPT_FLG)
      printf(" [Y|N] ");
    else
      printf("       ");
    printf("%*s-- %s", w_adjust, " ", o[i].usage);
    if (static_cast<unsigned int>(o[i].flags) & OPT_NOP)
      printf(" NOT IMPLEMENTED");
    printf("\n");
  }

  if (nCount == 0)
    printf("None defined.\n");

  return;
}

/*
 * Routine: di_compare()
 * Purpose: comparison routine for two d_idx_t entries; used by qsort
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
int di_compare(const void* op1, const void* op2) {
  d_idx_t *ie1 = (d_idx_t*)op1, *ie2 = (d_idx_t*)op2;

  return (strcasecmp(ie1->name, ie2->name));
}

int load_dists() {
  /* open the dist file */
  auto read_ptr = tpcds_idx;
  int32_t temp;
  memcpy(&temp, read_ptr, sizeof(int32_t));
  read_ptr += sizeof(int32_t);
  int entry_count = ntohl(temp);
  read_ptr = tpcds_idx + tpcds_idx_len - (entry_count * IDX_SIZE);
  for (int i = 0; i < entry_count; i++) {
    d_idx_t entry;
    memset(&entry, 0, sizeof(const d_idx_t));
    memcpy(entry.name, read_ptr, D_NAME_LEN);
    read_ptr += D_NAME_LEN;
    entry.name[D_NAME_LEN] = '\0';
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.index = ntohl(temp);
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.offset = ntohl(temp);
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.str_space = ntohl(temp);
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.length = ntohl(temp);
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.w_width = ntohl(temp);
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.v_width = ntohl(temp);
    memcpy(&temp, read_ptr, sizeof(int32_t));
    read_ptr += sizeof(int32_t);
    entry.name_space = ntohl(temp);
    load_dist(&entry);
    auto lockedMap = idx_.wlock();
    lockedMap->emplace(std::string(entry.name), entry);
  }
  return (1);
}
/*
 * Routine: find_dist(char *name, DSDGenContext& dsdGenContext)
 * Purpose: translate from dist_t name to d_idx_t *
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
const d_idx_t* find_dist(const char* name) {
  std::call_once(initFlag_, []() { load_dists(); });

  std::string key(name);
  auto lockedMap = idx_.rlock();
  if (lockedMap->find(key) != lockedMap->end()) {
    return &(lockedMap->at(key));
  }
  return nullptr;
}

/*
 * Routine: load_dist(int fd, dist_t *d, DSDGenContext& dsdGenContext)
 * Purpose: load a particular distribution
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
static int load_dist(d_idx_t* di) {
  int res = 0, i = 0, j = 0;
  dist_t* d;
  int32_t temp;
  FILE* ifp;

  if (di->flags != FL_LOADED) /* make sure no one beat us to it */
  {
    auto read_ptr = tpcds_idx;
    read_ptr += di->offset;
    di->dist = static_cast<dist_t*>(malloc(sizeof(struct DIST_T)));
    MALLOC_CHECK(di->dist);
    d = di->dist;

    /* load the type information */
    d->type_vector = static_cast<int*>(malloc(sizeof(int32_t) * di->v_width));
    MALLOC_CHECK(d->type_vector);
    for (i = 0; i < di->v_width; i++) {
      if (read_ptr)
        memcpy(&temp, read_ptr, sizeof(int32_t));
      read_ptr += sizeof(int32_t);
      d->type_vector[i] = ntohl(temp);
    }

    /* load the weights */
    d->weight_sets = static_cast<int**>(malloc(sizeof(int*) * di->w_width));
    d->maximums = static_cast<int*>(malloc(sizeof(int32_t) * di->w_width));
    MALLOC_CHECK(d->weight_sets);
    MALLOC_CHECK(d->maximums);
    for (i = 0; i < di->w_width; i++) {
      *(d->weight_sets + i) =
          static_cast<int*>(malloc(di->length * sizeof(int32_t)));
      MALLOC_CHECK(*(d->weight_sets + i));
      d->maximums[i] = 0;
      for (j = 0; j < di->length; j++) {
        if (read_ptr)
          memcpy(&temp, read_ptr, sizeof(int32_t));
        read_ptr += sizeof(int32_t);
        *(*(d->weight_sets + i) + j) = ntohl(temp);
        /* calculate the maximum weight and convert sets to cummulative
         */
        d->maximums[i] += d->weight_sets[i][j];
        d->weight_sets[i][j] = d->maximums[i];
      }
    }

    /* load the value offsets */
    d->value_sets = static_cast<int**>(malloc(sizeof(int*) * di->v_width));
    MALLOC_CHECK(d->value_sets);
    for (i = 0; i < di->v_width; i++) {
      *(d->value_sets + i) =
          static_cast<int*>(malloc(di->length * sizeof(int32_t)));
      MALLOC_CHECK(*(d->value_sets + i));
      for (j = 0; j < di->length; j++) {
        if (read_ptr)
          memcpy(&temp, read_ptr, sizeof(int32_t));
        read_ptr += sizeof(int32_t);
        *(*(d->value_sets + i) + j) = ntohl(temp);
      }
    }

    /* load the column aliases, if they were defined */
    if (di->name_space) {
      d->names = static_cast<char*>(malloc(di->name_space));
      MALLOC_CHECK(d->names);
      if (d->names && read_ptr)
        memcpy(d->names, read_ptr, di->name_space * sizeof(char));
      read_ptr += di->name_space * sizeof(char);
    }

    /* and finally the values themselves */
    d->strings = static_cast<char*>(malloc(sizeof(char) * di->str_space));
    MALLOC_CHECK(d->strings);
    if (d->strings && read_ptr)
      memcpy(d->strings, read_ptr, di->str_space * sizeof(char));
    read_ptr += di->str_space * sizeof(char);
    di->flags = FL_LOADED;
  }

  return (res);
}

/*
 * Routine: void *dist_op()
 * Purpose: select a value/weight from a distribution
 * Algorithm:
 * Data Structures:
 *
 * Params:	char *d_name
 *			int vset: which set of values
 *			int wset: which set of weights
 * Returns: appropriate data type cast as a void *
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20000317 Need to be sure this is portable to NT and others
 */
int dist_op(
    void* dest,
    int op,
    const char* d_name,
    int vset,
    int wset,
    int stream,
    DSDGenContext& dsdGenContext) {
  dist_t* dist = nullptr;
  int level = 0, index = 0, dt = 0;
  char* char_val = nullptr;
  int i_res = 1;
  const d_idx_t* d;

  if ((d = find_dist(d_name)) == nullptr) {
    char msg[40 + strlen(d_name)];
    auto result = sprintf(msg, "Invalid distribution name '%s'", d_name);
    if (result < 0)
      perror("sprintf failed");
    INTERNAL(msg);
    assert(d != nullptr);
  }

  dist = d->dist;

  if (op == 0) {
    genrand_integer(
        &level,
        DIST_UNIFORM,
        1,
        dist->maximums[wset - 1],
        0,
        stream,
        dsdGenContext);
    while (level > dist->weight_sets[wset - 1][index] && index < d->length)
      index += 1;
    dt = vset - 1;
    if ((index >= d->length) || (dt > d->v_width))
      INTERNAL("Distribution overrun");
    char_val = dist->strings + dist->value_sets[dt][index];
  } else {
    index = vset - 1;
    dt = wset - 1;
    if (index >= d->length || index < 0) {
      auto result =
          fprintf(stderr, "Runtime ERROR: Distribution over-run/under-run\n");
      if (result < 0)
        perror("sprintf failed");
      result = fprintf(
          stderr,
          "Check distribution definitions and usage for %s.\n",
          d->name);
      if (result < 0)
        perror("sprintf failed");
      result = fprintf(stderr, "index = %d, length=%d.\n", index, d->length);
      if (result < 0)
        perror("sprintf failed");
      exit(1);
    }
    char_val = dist->strings + dist->value_sets[dt][index];
  }

  switch (dist->type_vector[dt]) {
    case TKN_VARCHAR:
      if (dest)
        *static_cast<char**>(dest) = static_cast<char*>(char_val);
      break;
    case TKN_INT:
      i_res = atoi(char_val);
      if (dest)
        *static_cast<int*>(dest) = i_res;
      break;
    case TKN_DATE:
      if (dest == NULL) {
        dest = static_cast<date_t*>(malloc(sizeof(date_t)));
        MALLOC_CHECK(dest);
      }
      strtodt(*static_cast<date_t**>(dest), char_val);
      break;
    case TKN_DECIMAL:
      if (dest == NULL) {
        dest = static_cast<decimal_t*>(malloc(sizeof(decimal_t)));
        MALLOC_CHECK(dest);
      }
      strtodec(*static_cast<decimal_t**>(dest), char_val);
      break;
  }

  return (
      (dest == nullptr)
          ? i_res
          : index + 1); /* shift back to the 1-based indexing scheme */
}

/*
 * Routine: int dist_weight
 * Purpose: return the weight of a particular member of a distribution
 * Algorithm:
 * Data Structures:
 *
 * Params:	distribution *d
 *			int index: which "row"
 *			int wset: which set of weights
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 *	20000405 need to add error checking
 */
int dist_weight(
    int* dest,
    const char* d,
    int index,
    int wset,
    DSDGenContext& /*dsdGenContext*/) {
  dist_t* dist = nullptr;
  int res = 0;
  const d_idx_t* d_idx;

  if ((d_idx = find_dist(d)) == nullptr) {
    char msg[40 + strlen(d)];
    sprintf(msg, "Invalid distribution name '%s'", d);
    INTERNAL(msg);
  }

  dist = d_idx->dist;
  assert(index > 0);
  assert(wset > 0);
  res = dist->weight_sets[wset - 1][index - 1];
  /* reverse the accumulation of weights */
  if (index > 1)
    res -= dist->weight_sets[wset - 1][index - 2];

  if (dest == nullptr)
    return (res);

  *dest = res;

  return (0);
}

/*
 * Routine: int DistNameIndex()
 * Purpose: return the index of a column alias
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
int DistNameIndex(
    const char* szDist,
    int nNameType,
    const char* szName,
    DSDGenContext& /*dsdGenContext*/) {
  const d_idx_t* d_idx = nullptr;
  dist_t* dist = nullptr;
  int res = 0;
  char* cp = NULL;

  if ((d_idx = find_dist(szDist)) == NULL)
    return (-1);
  dist = d_idx->dist;

  if (dist->names == NULL)
    return (-1);

  res = 0;
  cp = dist->names;
  do {
    if (strcasecmp(szName, cp) == 0)
      break;
    cp += strlen(cp) + 1;
    res += 1;
  } while (res < (d_idx->v_width + d_idx->w_width));

  if (res >= 0) {
    if ((nNameType == VALUE_NAME) && (res < d_idx->v_width))
      return (res + 1);
    if ((nNameType == WEIGHT_NAME) && (res > d_idx->v_width))
      return (res - d_idx->v_width + 1);
  }

  return (-1);
}

/*
 * Routine: int distsize(char *name)
 * Purpose: return the size of a distribution
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
 *	20000405 need to add error checking
 */
int distsize(const char* name, DSDGenContext& /*dsdGenContext*/) {
  const d_idx_t* dist;

  dist = find_dist(name);

  if (dist == NULL)
    return (-1);

  return (dist->length);
}

/*
 * Routine: int IntegrateDist(char *szDistName, int nPct, int nStartIndex, int
 *nWeightSet) Purpose: return the index of the entry which, starting from
 *nStartIndex, would create a range comprising nPct of the total contained in
 *nWeightSet NOTE: the value can "wrap" -- that is, the returned value can be
 *less than nStartIndex Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 */

int IntegrateDist(
    const char* szDistName,
    int nPct,
    int nStartIndex,
    int nWeightSet,
    DSDGenContext& dsdGenContext) {
  const d_idx_t* pDistIndex = nullptr;
  int nGoal = 0, nSize = 0;

  if ((nPct <= 0) || (nPct >= 100))
    return (QERR_RANGE_ERROR);

  pDistIndex = find_dist(szDistName);
  if (pDistIndex == NULL)
    return (QERR_BAD_NAME);

  if (nStartIndex > pDistIndex->length)
    return (QERR_RANGE_ERROR);

  nGoal = pDistIndex->dist->maximums[nWeightSet];
  nGoal = nGoal * nPct / 100;
  nSize = distsize(szDistName, dsdGenContext);

  while (nGoal >= 0) {
    nStartIndex++;
    nGoal -= dist_weight(
        nullptr, szDistName, nStartIndex % nSize, nWeightSet, dsdGenContext);
  }

  return (nStartIndex);
}

/*
 * Routine: int dist_type(char *name, int nValueSet)
 * Purpose: return the type of the n-th value set in a distribution
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
int dist_type(const char* name, int nValueSet, DSDGenContext& dsdGenContext) {
  const d_idx_t* dist;

  dist = find_dist(name);

  if (dist == nullptr)
    return (-1);

  if (nValueSet < 1 || nValueSet > dist->v_width)
    return (-1);

  return (dist->dist->type_vector[nValueSet - 1]);
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
void dump_dist(const char* name, DSDGenContext& dsdGenContext) {
  const d_idx_t* pIndex = nullptr;
  int i, j;
  char* pCharVal = nullptr;
  int nVal = 0;

  pIndex = find_dist(name);
  if (pIndex == nullptr)
    ReportErrorNoLine(QERR_BAD_NAME, name, 1);
  printf("create %s;\n", pIndex->name);
  printf("set types = (");
  for (i = 0; i < pIndex->v_width; i++) {
    if (i > 0)
      printf(", ");
    printf(
        "%s", dist_type(name, i + 1, dsdGenContext) == 7 ? "int" : "varchar");
  }
  printf(");\n");
  printf("set weights = %d;\n", pIndex->w_width);
  for (i = 0; i < pIndex->length; i++) {
    printf("add(");
    for (j = 0; j < pIndex->v_width; j++) {
      if (j)
        printf(", ");
      if (dist_type(name, j + 1, dsdGenContext) != 7) {
        dist_member(&pCharVal, name, i + 1, j + 1, dsdGenContext);
        printf("\"%s\"", pCharVal);
      } else {
        dist_member(&nVal, name, i + 1, j + 1, dsdGenContext);
        printf("%d", nVal);
      }
    }
    printf("; ");
    for (j = 0; j < pIndex->w_width; j++) {
      if (j)
        printf(", ");
      printf("%d", dist_weight(nullptr, name, i + 1, j + 1, dsdGenContext));
    }
    printf(");\n");
  }

  return;
}

/*
 * Routine: dist_active(char *szName, int nWeightSet)
 * Purpose: return number of entries with non-zero weght values
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
int dist_active(
    const char* szName,
    int nWeightSet,
    DSDGenContext& dsdGenContext) {
  int nSize, nResult = 0, i = 0;

  nSize = distsize(szName, dsdGenContext);
  for (i = 1; i <= nSize; i++) {
    if (dist_weight(nullptr, szName, i, nWeightSet, dsdGenContext) != 0)
      nResult += 1;
  }

  return (nResult);
}

/*
 * Routine: DistSizeToShiftWidth(char *szDist)
 * Purpose: Determine the number of bits required to select a member of the
 * distribution Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int DistSizeToShiftWidth(
    const char* szDist,
    int nWeightSet,
    DSDGenContext& dsdGenContext) {
  int nBits = 1, nMax;
  unsigned int nTotal = 2;
  const d_idx_t* d;

  d = find_dist(szDist);
  nMax = dist_max(d->dist, nWeightSet, dsdGenContext);

  while (nTotal < nMax) {
    nBits += 1;
    nTotal <<= 1;
  }

  return (nBits);
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
int MatchDistWeight(
    void* dest,
    const char* szDist,
    int nWeight,
    int nWeightSet,
    int ValueSet,
    DSDGenContext& dsdGenContext) {
  const d_idx_t* d = nullptr;
  dist_t* dist = nullptr;
  int index = 0, dt = 0, i_res = 0, nRetcode = 0;
  char* char_val;

  if ((d = find_dist(szDist)) == nullptr) {
    char msg[40 + strlen(szDist)];
    sprintf(msg, "Invalid distribution name '%s'", szDist);
    INTERNAL(msg);
  }

  dist = d->dist;
  nWeight %= dist->maximums[nWeightSet - 1];

  while (nWeight > dist->weight_sets[nWeightSet - 1][index] &&
         index < d->length)
    index += 1;
  dt = ValueSet - 1;
  if (index >= d->length)
    index = d->length - 1;
  char_val = dist->strings + dist->value_sets[dt][index];

  switch (dist->type_vector[dt]) {
    case TKN_VARCHAR:
      if (dest)
        *static_cast<char**>(dest) = static_cast<char*>(char_val);
      break;
    case TKN_INT:
      i_res = atoi(char_val);
      if (dest)
        *static_cast<int*>(dest) = i_res;
      break;
    case TKN_DATE:
      if (dest == nullptr) {
        dest = static_cast<date_t*>(malloc(sizeof(date_t)));
        MALLOC_CHECK(dest);
      }
      strtodt(*static_cast<date_t**>(dest), char_val);
      break;
    case TKN_DECIMAL:
      if (dest == nullptr) {
        dest = static_cast<decimal_t*>(malloc(sizeof(decimal_t)));
        MALLOC_CHECK(dest);
      }
      strtodec(*(decimal_t**)dest, char_val);
      break;
  }

  nRetcode = 1;
  index = 1;
  while (index < dist->maximums[nWeightSet - 1]) {
    nRetcode += 1;
    index *= 2;
  }

  return (nRetcode);
}

/*
 * Routine: findDistValue(char *szValue, char *szDistName, int nValueSet)
 * Purpose: Return the row number where the entry is found
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
 * 20031024 jms this routine needs to handle all data types, not just varchar
 */
int findDistValue(
    const char* szValue,
    const char* szDistName,
    int ValueSet,
    DSDGenContext& dsdGenContext) {
  char szDistValue[128];
  int nRetValue = 1, nDistMax;

  nDistMax = distsize(szDistName, dsdGenContext);

  for (nRetValue = 1; nRetValue < nDistMax; nRetValue++) {
    dist_member(&szDistValue, szDistName, nRetValue, ValueSet, dsdGenContext);
    if (strcmp(szValue, szDistValue) == 0)
      break;
  }

  if (nRetValue <= nDistMax)
    return (nRetValue);
  return (-1);
}

void DSDGenContext::Reset() {
  init_rand_init = 0;
  mk_address_init = 0;
  setUpdateDateRange_init = 0;
  mk_dbgen_version_init = 0;
  getCatalogNumberFromPage_init = 0;
  checkSeeds_init = 0;
  dateScaling_init = 0;
  mk_w_call_center_init = 0;
  mk_w_catalog_page_init = 0;
  mk_master_catalog_sales_init = 0;
  dectostr_init = 0;
  date_join_init = 0;
  setSCDKeys_init = 0;
  scd_join_init = 0;
  matchSCDSK_init = 0;
  skipDays_init = 0;
  mk_w_catalog_returns_init = 0;
  mk_detail_catalog_sales_init = 0;
  mk_w_customer_init = 0;
  mk_w_date_init = 0;
  mk_w_inventory_init = 0;
  mk_w_item_init = 0;
  mk_w_promotion_init = 0;
  mk_w_reason_init = 0;
  mk_w_ship_mode_init = 0;
  mk_w_store_returns_init = 0;
  mk_master_store_sales_init = 0;
  mk_w_store_init = 0;
  mk_w_web_page_init = 0;
  mk_w_web_returns_init = 0;
  mk_master_init = 0;
  mk_detail_init = 0;
  mk_w_web_site_init = 0;
  mk_cust_init = 0;
  mk_order_init = 0;
  mk_part_init = 0;
  mk_supp_init = 0;
  dbg_text_init = 0;
  find_dist_init = 0;
  cp_join_init = 0;
  web_join_init = 0;
  set_pricing_init = 0;
  init_params_init = 0;
  get_rowcount_init = 0;
  mk_detail_web_sales_init = 0;
  mk_master_web_sales_init = 0;
}

#ifdef TEST
main() {
  int i_res;
  char* c_res;
  decimal_t dec_res;

  init_params();

  dist_member(&i_res, "test_dist", 1, 1);
  if (i_res != 10) {
    printf("dist_member(\"test_dist\", 1, 1): %d != 10\n", i_res);
    exit(1);
  }
  dist_member(&i_res, "test_dist", 1, 2);
  if (i_res != 60) {
    printf("dist_member(\"test_dist\", 1, 2): %d != 60\n", i_res);
    exit(1);
  }
  dist_member((void*)&c_res, "test_dist", 1, 3);
  if (strcmp(c_res, "El Camino")) {
    printf("dist_member(\"test_dist\", 1, 3): %s != El Camino\n", c_res);
    exit(1);
  }
  dist_member((void*)&dec_res, "test_dist", 1, 4);
  if (strcmp(dec_res.number, "1") || strcmp(dec_res.fraction, "23")) {
    printf(
        "dist_member(\"test_dist\", 1, 4): %s.%s != 1.23\n",
        dec_res.number,
        dec_res.fraction);
    exit(1);
  }
  dist_weight(&i_res, "test_dist", 2, 2);
  if (3 != i_res) {
    printf("dist_weight(\"test_dist\", 2, 2): %d != 3\n", i_res);
    exit(1);
  }
}
#endif /* TEST */
