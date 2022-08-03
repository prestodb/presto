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

package com.facebook.presto.hive.functions;

import com.facebook.airlift.log.Logger;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCollectList;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCollectSet;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFComputeStats;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFContextNGrams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCorrelation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCovariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCovarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCumeDist;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFDenseRank;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFFirstValue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFHistogramNumeric;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLastValue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFNTile;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentRank;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFRank;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFRowNumber;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStdSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFnGrams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAddMonths;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAesDecrypt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAesEncrypt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayContains;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAssertTrue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAssertTrueOOM;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBRound;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCardinalityViolation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCbrt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCeil;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCharacterLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcatWS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCurrentDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCurrentGroups;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCurrentTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCurrentUser;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateAdd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateDiff;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateFormat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateSub;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDecode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFElt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEncode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEnforceConstraint;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEpochMilli;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFExtractUnion;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFactorial;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFField;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFloor;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFormatNumber;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFromUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFGreatest;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFGrouping;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInFile;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInitCap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInstr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInternalInterval;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLTrim;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLastDay;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLead;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeast;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLevenshtein;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLikeAll;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLikeAny;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLocate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLoggedInUser;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLpad;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapKeys;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapValues;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMonthsBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNamedStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNullif;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNvl;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPFalse;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotFalse;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotTrue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPositive;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPTrue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOctetLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFPosMod;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFPower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFQuarter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRTrim;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRegExp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRestrictInformationSchema;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRound;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRpad;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSQCountCheck;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSentences;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSha2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSize;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArrayByField;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSoundex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSplit;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSubstringIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToIntervalDayTime;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToIntervalYearMonth;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToTimestampLocalTZ;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTranslate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTrim;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTrunc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnion;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWidthBucket;
import org.apache.hadoop.hive.ql.udf.generic.UDFCurrentDB;
import org.apache.hadoop.util.ReflectionUtils;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

public final class FunctionRegistry
{
    private FunctionRegistry() {}

    private static final Logger log = Logger.get(FunctionRegistry.class);

    // registry for hive functions
    public static final Registry system = new Registry(true);

    static {
        // register genericUDF
        system.registerGenericUDF("concat", GenericUDFConcat.class);
        system.registerGenericUDF("substring_index", GenericUDFSubstringIndex.class);
        system.registerGenericUDF("lpad", GenericUDFLpad.class);
        system.registerGenericUDF("rpad", GenericUDFRpad.class);
        system.registerGenericUDF("levenshtein", GenericUDFLevenshtein.class);
        system.registerGenericUDF("soundex", GenericUDFSoundex.class);
        system.registerGenericUDF("size", GenericUDFSize.class);
        system.registerGenericUDF("round", GenericUDFRound.class);
        system.registerGenericUDF("bround", GenericUDFBRound.class);
        system.registerGenericUDF("floor", GenericUDFFloor.class);
        system.registerGenericUDF("cbrt", GenericUDFCbrt.class);
        system.registerGenericUDF("ceil", GenericUDFCeil.class);
        system.registerGenericUDF("ceiling", GenericUDFCeil.class);
        system.registerGenericUDF("abs", GenericUDFAbs.class);
        system.registerGenericUDF("sq_count_check", GenericUDFSQCountCheck.class);
        system.registerGenericUDF("enforce_constraint", GenericUDFEnforceConstraint.class);
        system.registerGenericUDF("pmod", GenericUDFPosMod.class);
        system.registerGenericUDF("power", GenericUDFPower.class);
        system.registerGenericUDF("pow", GenericUDFPower.class);
        system.registerGenericUDF("factorial", GenericUDFFactorial.class);
        system.registerGenericUDF("sha2", GenericUDFSha2.class);
        system.registerGenericUDF("aes_encrypt", GenericUDFAesEncrypt.class);
        system.registerGenericUDF("aes_decrypt", GenericUDFAesDecrypt.class);
        system.registerGenericUDF("encode", GenericUDFEncode.class);
        system.registerGenericUDF("decode", GenericUDFDecode.class);
        system.registerGenericUDF("upper", GenericUDFUpper.class);
        system.registerGenericUDF("lower", GenericUDFLower.class);
        system.registerGenericUDF("ucase", GenericUDFUpper.class);
        system.registerGenericUDF("lcase", GenericUDFLower.class);
        system.registerGenericUDF("trim", GenericUDFTrim.class);
        system.registerGenericUDF("ltrim", GenericUDFLTrim.class);
        system.registerGenericUDF("rtrim", GenericUDFRTrim.class);
        system.registerGenericUDF("length", GenericUDFLength.class);
        system.registerGenericUDF("character_length", GenericUDFCharacterLength.class);
        system.registerGenericUDF("char_length", GenericUDFCharacterLength.class);
        system.registerGenericUDF("octet_length", GenericUDFOctetLength.class);
        system.registerGenericUDF("field", GenericUDFField.class);
        system.registerGenericUDF("initcap", GenericUDFInitCap.class);
        system.registerGenericUDF("likeany", GenericUDFLikeAny.class);
        system.registerGenericUDF("likeall", GenericUDFLikeAll.class);
        system.registerGenericUDF("rlike", GenericUDFRegExp.class);
        system.registerGenericUDF("regexp", GenericUDFRegExp.class);
        system.registerGenericUDF("nvl", GenericUDFNvl.class);
        system.registerGenericUDF("split", GenericUDFSplit.class);
        system.registerGenericUDF("str_to_map", GenericUDFStringToMap.class);
        system.registerGenericUDF("translate", GenericUDFTranslate.class);
        system.registerGenericUDF("positive", GenericUDFOPPositive.class);
        system.registerGenericUDF("negative", GenericUDFOPNegative.class);
        system.registerGenericUDF("quarter", GenericUDFQuarter.class);
        system.registerGenericUDF("to_date", GenericUDFDate.class);
        system.registerGenericUDF("last_day", GenericUDFLastDay.class);
        system.registerGenericUDF("next_day", GenericUDFNextDay.class);
        system.registerGenericUDF("trunc", GenericUDFTrunc.class);
        system.registerGenericUDF("date_format", GenericUDFDateFormat.class);
        system.registerGenericUDF("date_add", GenericUDFDateAdd.class);
        system.registerGenericUDF("date_sub", GenericUDFDateSub.class);
        system.registerGenericUDF("datediff", GenericUDFDateDiff.class);
        system.registerGenericUDF("add_months", GenericUDFAddMonths.class);
        system.registerGenericUDF("months_between", GenericUDFMonthsBetween.class);
        // system.registerGenericUDF("xpath", GenericUDFXPath.class);
        system.registerGenericUDF("grouping", GenericUDFGrouping.class);
        system.registerGenericUDF("current_database", UDFCurrentDB.class);
        system.registerGenericUDF("current_date", GenericUDFCurrentDate.class);
        system.registerGenericUDF("current_timestamp", GenericUDFCurrentTimestamp.class);
        system.registerGenericUDF("current_user", GenericUDFCurrentUser.class);
        system.registerGenericUDF("current_groups", GenericUDFCurrentGroups.class);
        system.registerGenericUDF("logged_in_user", GenericUDFLoggedInUser.class);
        system.registerGenericUDF("restrict_information_schema", GenericUDFRestrictInformationSchema.class);
        system.registerGenericUDF("isnull", GenericUDFOPNull.class);
        system.registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);
        system.registerGenericUDF("istrue", GenericUDFOPTrue.class);
        system.registerGenericUDF("isnottrue", GenericUDFOPNotTrue.class);
        system.registerGenericUDF("isfalse", GenericUDFOPFalse.class);
        system.registerGenericUDF("isnotfalse", GenericUDFOPNotFalse.class);
        system.registerGenericUDF("between", GenericUDFBetween.class);
        system.registerGenericUDF("in_bloom_filter", GenericUDFInBloomFilter.class);
        system.registerGenericUDF("date", GenericUDFToDate.class);
        system.registerGenericUDF("timestamp", GenericUDFTimestamp.class);
        system.registerGenericUDF("timestamp with local time zone", GenericUDFToTimestampLocalTZ.class);
        system.registerGenericUDF("interval_year_month", GenericUDFToIntervalYearMonth.class);
        system.registerGenericUDF("interval_day_time", GenericUDFToIntervalDayTime.class);
        system.registerGenericUDF("binary", GenericUDFToBinary.class);
        system.registerGenericUDF("decimal", GenericUDFToDecimal.class);
        system.registerGenericUDF("varchar", GenericUDFToVarchar.class);
        system.registerGenericUDF("char", GenericUDFToChar.class);
        system.registerGenericUDF("reflect", GenericUDFReflect.class);
        system.registerGenericUDF("reflect2", GenericUDFReflect2.class);
        system.registerGenericUDF("java_method", GenericUDFReflect.class);
        system.registerGenericUDF("array", GenericUDFArray.class);
        system.registerGenericUDF("assert_true", GenericUDFAssertTrue.class);
        system.registerGenericUDF("assert_true_oom", GenericUDFAssertTrueOOM.class);
        system.registerGenericUDF("map", GenericUDFMap.class);
        system.registerGenericUDF("struct", GenericUDFStruct.class);
        system.registerGenericUDF("named_struct", GenericUDFNamedStruct.class);
        system.registerGenericUDF("create_union", GenericUDFUnion.class);
        system.registerGenericUDF("extract_union", GenericUDFExtractUnion.class);
        system.registerGenericUDF("case", GenericUDFCase.class);
        system.registerGenericUDF("when", GenericUDFWhen.class);
        system.registerGenericUDF("nullif", GenericUDFNullif.class);
        system.registerGenericUDF("hash", GenericUDFHash.class);
        system.registerGenericUDF("murmur_hash", GenericUDFMurmurHash.class);
        system.registerGenericUDF("coalesce", GenericUDFCoalesce.class);
        system.registerGenericUDF("index", GenericUDFIndex.class);
        system.registerGenericUDF("in_file", GenericUDFInFile.class);
        system.registerGenericUDF("instr", GenericUDFInstr.class);
        system.registerGenericUDF("locate", GenericUDFLocate.class);
        system.registerGenericUDF("elt", GenericUDFElt.class);
        system.registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
        system.registerGenericUDF("sort_array", GenericUDFSortArray.class);
        system.registerGenericUDF("sort_array_by", GenericUDFSortArrayByField.class);
        system.registerGenericUDF("array_contains", GenericUDFArrayContains.class);
        system.registerGenericUDF("sentences", GenericUDFSentences.class);
        system.registerGenericUDF("map_keys", GenericUDFMapKeys.class);
        system.registerGenericUDF("map_values", GenericUDFMapValues.class);
        system.registerGenericUDF("format_number", GenericUDFFormatNumber.class);
        system.registerGenericUDF("printf", GenericUDFPrintf.class);
        system.registerGenericUDF("greatest", GenericUDFGreatest.class);
        system.registerGenericUDF("least", GenericUDFLeast.class);
        system.registerGenericUDF("cardinality_violation", GenericUDFCardinalityViolation.class);
        system.registerGenericUDF("width_bucket", GenericUDFWidthBucket.class);
        system.registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
        system.registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);
        system.registerGenericUDF("unix_timestamp", GenericUDFUnixTimeStamp.class);
        system.registerGenericUDF("to_unix_timestamp", GenericUDFToUnixTimeStamp.class);
        system.registerGenericUDF("internal_interval", GenericUDFInternalInterval.class);
        system.registerGenericUDF("to_epoch_milli", GenericUDFEpochMilli.class);
        system.registerGenericUDF("lead", GenericUDFLead.class);
        system.registerGenericUDF("lag", GenericUDFLag.class);

        // register UDF
        system.registerUDF("asin", UDFAsin.class, false);
        system.registerUDF("acos", UDFAcos.class, false);

        // Aggregate functions
        system.registerGenericUDAF("max", new GenericUDAFMax());
        system.registerGenericUDAF("min", new GenericUDAFMin());
        system.registerGenericUDAF("sum", new GenericUDAFSum());
        system.registerGenericUDAF("count", new GenericUDAFCount());
        system.registerGenericUDAF("avg", new GenericUDAFAverage());
        system.registerGenericUDAF("std", new GenericUDAFStd());
        system.registerGenericUDAF("stddev", new GenericUDAFStd());
        system.registerGenericUDAF("stddev_pop", new GenericUDAFStd());
        system.registerGenericUDAF("stddev_samp", new GenericUDAFStdSample());
        system.registerGenericUDAF("variance", new GenericUDAFVariance());
        system.registerGenericUDAF("var_pop", new GenericUDAFVariance());
        system.registerGenericUDAF("var_samp", new GenericUDAFVarianceSample());
        system.registerGenericUDAF("covar_pop", new GenericUDAFCovariance());
        system.registerGenericUDAF("covar_samp", new GenericUDAFCovarianceSample());
        system.registerGenericUDAF("corr", new GenericUDAFCorrelation());
        system.registerGenericUDAF("histogram_numeric", new GenericUDAFHistogramNumeric());
        system.registerGenericUDAF("percentile_approx", new GenericUDAFPercentileApprox());
        system.registerGenericUDAF("collect_set", new GenericUDAFCollectSet());
        system.registerGenericUDAF("collect_list", new GenericUDAFCollectList());
        system.registerGenericUDAF("ngrams", new GenericUDAFnGrams());
        system.registerGenericUDAF("context_ngrams", new GenericUDAFContextNGrams());
        system.registerGenericUDAF("compute_stats", new GenericUDAFComputeStats());
        system.registerGenericUDAF("row_number", new GenericUDAFRowNumber());
        system.registerGenericUDAF("rank", new GenericUDAFRank());
        system.registerGenericUDAF("dense_rank", new GenericUDAFDenseRank());
        system.registerGenericUDAF("percent_rank", new GenericUDAFPercentRank());
        system.registerGenericUDAF("cume_dist", new GenericUDAFCumeDist());
        system.registerGenericUDAF("ntile", new GenericUDAFNTile());
        system.registerGenericUDAF("first_value", new GenericUDAFFirstValue());
        system.registerGenericUDAF("last_value", new GenericUDAFLastValue());

        File file = new File("/tmp/core-udfs-1.0-SNAPSHOT-standalone.jar");
        URL url;
        try {
            url = file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        try (URLClassLoader childClassLoader = new URLClassLoader(new URL[]{url}, ClassLoader.getSystemClassLoader())) {
            Class<?> urlClass = URLClassLoader.class;
            Method method = urlClass.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(childClassLoader, url);

            ConfigurationBuilder configurationBuilder =
                    new ConfigurationBuilder()
                            .addClassLoaders(childClassLoader)
                            .setScanners(new SubTypesScanner(false))
                            .addUrls(ClasspathHelper.forPackage("com.facebook", childClassLoader));
                            // .addUrls(ClasspathHelper.forPackage("org.apache", childClassLoader));
            Reflections reflections = new Reflections(configurationBuilder);

            // Get and Register UDF
            registerHiveUDFs(reflections.getSubTypesOf(UDF.class));

            // Get and Register GenericUDF
            registerHiveGenericUDFs(reflections.getSubTypesOf(GenericUDF.class));

//            log.info("All udfs: " + result);
//            for (Class<?> udf : result) {
//                if (!udf.getName().startsWith("org.apache.hadoop.hive")) {
//                    log.info("registering " + udfClassToFBName(udf.getSimpleName()));
//
//                    try {
//                        registerTemporaryHiveFunction(udfClassToFBName(udf.getSimpleName()), udf);
//                    } catch (Throwable e) {
//                        log.info("Class not found: " + e.getMessage());
//                    }
//                }
//            }
//            result.size();
        }
        catch (IOException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String udfClassToFBName(String udfClass) {
        return "FB_"+udfClass.replaceFirst("^UD(A|T)?F", "").replaceAll(
                String.format("%s|%s|%s",
                        "(?<=[A-Z])(?=[A-Z][a-z])",
                        "(?<=[^A-Z0-9])(?=[A-Z])",
                        "(?<=[A-Za-z0-9])(?=[^A-Za-z0-9])"
                ), "_").toUpperCase();
    }

    public static void registerHiveUDFs(Set<Class<? extends UDF>> udfs) {
        for (Class<? extends UDF> udfClass : udfs) {
            if (!udfClass.getName().startsWith("org.apache.hadoop.hive")) {
                String functionName = udfClassToFBName(udfClass.getSimpleName());
                log.info("registering " + functionName);

                try {
                    log.info("Registering function inside presto hive function namespace: " + functionName + " " + udfClass.getName());
                    if (system.isBuiltInFunc(udfClass)) {
                        log.info("Skipping builtin function " + functionName);
                        return;
                    }

                    log.info("Registering UDF inside Presto hive functions namespace: " + functionName);
                    system.registerUDF(functionName, udfClass, false);
                } catch (Throwable e) {
                    log.info("Class not found: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    public static void registerHiveGenericUDFs(Set<Class<? extends GenericUDF>> udfs) {
        for (Class<? extends GenericUDF> udfClass : udfs) {
            if (!udfClass.getName().startsWith("org.apache.hadoop.hive")) {
                String functionName = udfClassToFBName(udfClass.getSimpleName());
                log.info("registering " + functionName);

                try {
                    log.info("Registering function inside presto hive function namespace: " + functionName + " " + udfClass.getName());
                    if (system.isBuiltInFunc(udfClass)) {
                        log.info("Skipping builtin function " + functionName);
                        return;
                    }

                    log.info("Registering GenericUDF : " + functionName);
                    system.registerGenericUDF(functionName, udfClass);
                } catch (Throwable e) {
                    log.info("Class not found: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }


    }


    public static boolean registerTemporaryHiveFunction(
            String functionName, Class<?> udfClass)
    {
        log.info("Registering function inside presto hive function namespace: " + functionName + " " + udfClass.getName());
        if (system.isBuiltInFunc(udfClass)) {
            log.info("Skipping builtin function " + functionName);
            return false;
        }

        if (UDF.class.isAssignableFrom(udfClass)) {
            log.info("Registering UDF inside Presto hive functions namespace: " + functionName);
            system.registerUDF(functionName, (Class<? extends UDF>) udfClass, false);
        }
        else if (GenericUDF.class.isAssignableFrom(udfClass)) {
            log.info("Registering GenericUDF : " + functionName);
            system.registerGenericUDF(functionName, (Class<? extends GenericUDF>) udfClass);
        }
//        else if (GenericUDTF.class.isAssignableFrom(udfClass)) {
//            system.registerGenericUDTF(functionName, (Class<? extends GenericUDTF>) udfClass);
//        }
//        else if (UDAF.class.isAssignableFrom(udfClass)) {
//            system.registerUDAF(functionName, (Class<? extends UDAF>) udfClass);
//        }
        else if (GenericUDAFResolver.class.isAssignableFrom(udfClass)) {
            system.registerGenericUDAF(functionName, (GenericUDAFResolver)
                    ReflectionUtils.newInstance(udfClass, null));
        }
        else {
            log.info("Skipping registration for function: " + functionName);
            return false;
        }

        log.info("Suceessful registration for function: " + functionName);
        return true;
    }

    public static FunctionInfo getFunctionInfo(String functionName)
            throws SemanticException
    {
        return system.getFunctionInfo(functionName);
    }

    public static Set<String> getCurrentFunctionNames()
    {
        return system.getCurrentFunctionNames();
    }
}
