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

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBase64;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFChr;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFCrc32;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorDay;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorHour;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMinute;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMonth;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorQuarter;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorSecond;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorWeek;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorYear;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDayOfWeek;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFFromUnixTime;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMd5;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPBitNot;
import org.apache.hadoop.hive.ql.udf.UDFOPBitOr;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftLeft;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRight;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRightUnsigned;
import org.apache.hadoop.hive.ql.udf.UDFOPBitXor;
import org.apache.hadoop.hive.ql.udf.UDFOPLongDivide;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFParseUrl;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReplace;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSha1;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSpace;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFUUID;
import org.apache.hadoop.hive.ql.udf.UDFUnbase64;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFVersion;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFRowNumber;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStdSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFnGrams;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMask;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskFirstN;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskLastN;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowFirstN;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowLastN;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMonthsBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNamedStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNullif;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNvl;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDTIMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDTIPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPFalse;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotFalse;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotTrue;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNumericMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNumericPlus;
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
import org.apache.hadoop.hive.ql.udf.xml.GenericUDFXPath;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathBoolean;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathDouble;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathFloat;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathInteger;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathLong;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathShort;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathString;
import org.apache.hadoop.hive.serde.serdeConstants;

public class FunctionRegistry
{
    public static final String LEAD_FUNC_NAME = "lead";
    public static final String LAG_FUNC_NAME = "lag";

    public static final String UNARY_PLUS_FUNC_NAME = "positive";
    public static final String UNARY_MINUS_FUNC_NAME = "negative";

    // registry for system functions
    private static final Registry system = new Registry(true);

    static {
        system.registerGenericUDF("concat", GenericUDFConcat.class);
        system.registerUDF("substr", UDFSubstr.class, false);
        system.registerUDF("substring", UDFSubstr.class, false);
        system.registerGenericUDF("substring_index", GenericUDFSubstringIndex.class);
        system.registerUDF("space", UDFSpace.class, false);
        system.registerUDF("repeat", UDFRepeat.class, false);
        system.registerUDF("ascii", UDFAscii.class, false);
        system.registerGenericUDF("lpad", GenericUDFLpad.class);
        system.registerGenericUDF("rpad", GenericUDFRpad.class);
        system.registerGenericUDF("levenshtein", GenericUDFLevenshtein.class);
        system.registerGenericUDF("soundex", GenericUDFSoundex.class);

        system.registerGenericUDF("size", GenericUDFSize.class);

        system.registerGenericUDF("round", GenericUDFRound.class);
        system.registerGenericUDF("bround", GenericUDFBRound.class);
        system.registerGenericUDF("floor", GenericUDFFloor.class);
        system.registerUDF("sqrt", UDFSqrt.class, false);
        system.registerGenericUDF("cbrt", GenericUDFCbrt.class);
        system.registerGenericUDF("ceil", GenericUDFCeil.class);
        system.registerGenericUDF("ceiling", GenericUDFCeil.class);
        system.registerUDF("rand", UDFRand.class, false);
        system.registerGenericUDF("abs", GenericUDFAbs.class);
        system.registerGenericUDF("sq_count_check", GenericUDFSQCountCheck.class);
        system.registerGenericUDF("enforce_constraint", GenericUDFEnforceConstraint.class);
        system.registerGenericUDF("pmod", GenericUDFPosMod.class);

        system.registerUDF("ln", UDFLn.class, false);
        system.registerUDF("log2", UDFLog2.class, false);
        system.registerUDF("sin", UDFSin.class, false);
        system.registerUDF("asin", UDFAsin.class, false);
        system.registerUDF("cos", UDFCos.class, false);
        system.registerUDF("acos", UDFAcos.class, false);
        system.registerUDF("log10", UDFLog10.class, false);
        system.registerUDF("log", UDFLog.class, false);
        system.registerUDF("exp", UDFExp.class, false);
        system.registerGenericUDF("power", GenericUDFPower.class);
        system.registerGenericUDF("pow", GenericUDFPower.class);
        system.registerUDF("sign", UDFSign.class, false);
        system.registerUDF("pi", UDFPI.class, false);
        system.registerUDF("degrees", UDFDegrees.class, false);
        system.registerUDF("radians", UDFRadians.class, false);
        system.registerUDF("atan", UDFAtan.class, false);
        system.registerUDF("tan", UDFTan.class, false);
        system.registerUDF("e", UDFE.class, false);
        system.registerGenericUDF("factorial", GenericUDFFactorial.class);
        system.registerUDF("crc32", UDFCrc32.class, false);

        system.registerUDF("conv", UDFConv.class, false);
        system.registerUDF("bin", UDFBin.class, false);
        system.registerUDF("chr", UDFChr.class, false);
        system.registerUDF("hex", UDFHex.class, false);
        system.registerUDF("unhex", UDFUnhex.class, false);
        system.registerUDF("base64", UDFBase64.class, false);
        system.registerUDF("unbase64", UDFUnbase64.class, false);
        system.registerGenericUDF("sha2", GenericUDFSha2.class);
        system.registerUDF("md5", UDFMd5.class, false);
        system.registerUDF("sha1", UDFSha1.class, false);
        system.registerUDF("sha", UDFSha1.class, false);
        system.registerGenericUDF("aes_encrypt", GenericUDFAesEncrypt.class);
        system.registerGenericUDF("aes_decrypt", GenericUDFAesDecrypt.class);
        system.registerUDF("uuid", UDFUUID.class, false);

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
        system.registerUDF("reverse", UDFReverse.class, false);
        system.registerGenericUDF("field", GenericUDFField.class);
        system.registerUDF("find_in_set", UDFFindInSet.class, false);
        system.registerGenericUDF("initcap", GenericUDFInitCap.class);

        system.registerUDF("like", UDFLike.class, true);
        system.registerGenericUDF("likeany", GenericUDFLikeAny.class);
        system.registerGenericUDF("likeall", GenericUDFLikeAll.class);
        system.registerGenericUDF("rlike", GenericUDFRegExp.class);
        system.registerGenericUDF("regexp", GenericUDFRegExp.class);
        system.registerUDF("regexp_replace", UDFRegExpReplace.class, false);
        system.registerUDF("replace", UDFReplace.class, false);
        system.registerUDF("regexp_extract", UDFRegExpExtract.class, false);
        system.registerUDF("parse_url", UDFParseUrl.class, false);
        system.registerGenericUDF("nvl", GenericUDFNvl.class);
        system.registerGenericUDF("split", GenericUDFSplit.class);
        system.registerGenericUDF("str_to_map", GenericUDFStringToMap.class);
        system.registerGenericUDF("translate", GenericUDFTranslate.class);

        system.registerGenericUDF(UNARY_PLUS_FUNC_NAME, GenericUDFOPPositive.class);
        system.registerGenericUDF(UNARY_MINUS_FUNC_NAME, GenericUDFOPNegative.class);

        system.registerUDF("day", UDFDayOfMonth.class, false);
        system.registerUDF("dayofmonth", UDFDayOfMonth.class, false);
        system.registerUDF("dayofweek", UDFDayOfWeek.class, false);
        system.registerUDF("month", UDFMonth.class, false);
        system.registerGenericUDF("quarter", GenericUDFQuarter.class);
        system.registerUDF("year", UDFYear.class, false);
        system.registerUDF("hour", UDFHour.class, false);
        system.registerUDF("minute", UDFMinute.class, false);
        system.registerUDF("second", UDFSecond.class, false);
        system.registerUDF("from_unixtime", UDFFromUnixTime.class, false);
        system.registerGenericUDF("to_date", GenericUDFDate.class);
        system.registerUDF("weekofyear", UDFWeekOfYear.class, false);
        system.registerGenericUDF("last_day", GenericUDFLastDay.class);
        system.registerGenericUDF("next_day", GenericUDFNextDay.class);
        system.registerGenericUDF("trunc", GenericUDFTrunc.class);
        system.registerGenericUDF("date_format", GenericUDFDateFormat.class);

        // Special date formatting functions
        system.registerUDF("floor_year", UDFDateFloorYear.class, false);
        system.registerUDF("floor_quarter", UDFDateFloorQuarter.class, false);
        system.registerUDF("floor_month", UDFDateFloorMonth.class, false);
        system.registerUDF("floor_day", UDFDateFloorDay.class, false);
        system.registerUDF("floor_week", UDFDateFloorWeek.class, false);
        system.registerUDF("floor_hour", UDFDateFloorHour.class, false);
        system.registerUDF("floor_minute", UDFDateFloorMinute.class, false);
        system.registerUDF("floor_second", UDFDateFloorSecond.class, false);

        system.registerGenericUDF("date_add", GenericUDFDateAdd.class);
        system.registerGenericUDF("date_sub", GenericUDFDateSub.class);
        system.registerGenericUDF("datediff", GenericUDFDateDiff.class);
        system.registerGenericUDF("add_months", GenericUDFAddMonths.class);
        system.registerGenericUDF("months_between", GenericUDFMonthsBetween.class);

        system.registerUDF("get_json_object", UDFJson.class, false);

        system.registerUDF("xpath_string", UDFXPathString.class, false);
        system.registerUDF("xpath_boolean", UDFXPathBoolean.class, false);
        system.registerUDF("xpath_number", UDFXPathDouble.class, false);
        system.registerUDF("xpath_double", UDFXPathDouble.class, false);
        system.registerUDF("xpath_float", UDFXPathFloat.class, false);
        system.registerUDF("xpath_long", UDFXPathLong.class, false);
        system.registerUDF("xpath_int", UDFXPathInteger.class, false);
        system.registerUDF("xpath_short", UDFXPathShort.class, false);
        system.registerGenericUDF("xpath", GenericUDFXPath.class);

        system.registerUDF("div", UDFOPLongDivide.class, true);

        system.registerUDF("&", UDFOPBitAnd.class, true);
        system.registerUDF("|", UDFOPBitOr.class, true);
        system.registerUDF("^", UDFOPBitXor.class, true);
        system.registerUDF("~", UDFOPBitNot.class, true);
        system.registerUDF("shiftleft", UDFOPBitShiftLeft.class, true);
        system.registerUDF("shiftright", UDFOPBitShiftRight.class, true);
        system.registerUDF("shiftrightunsigned", UDFOPBitShiftRightUnsigned.class, true);

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

        // Utility UDFs
        system.registerUDF("version", UDFVersion.class, false);

        // Aliases for Java Class Names
        // These are used in getImplicitConvertUDFMethod
        system.registerUDF(serdeConstants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
        system.registerUDF(serdeConstants.TINYINT_TYPE_NAME, UDFToByte.class, false, UDFToByte.class.getSimpleName());
        system.registerUDF(serdeConstants.SMALLINT_TYPE_NAME, UDFToShort.class, false, UDFToShort.class.getSimpleName());
        system.registerUDF(serdeConstants.INT_TYPE_NAME, UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
        system.registerUDF(serdeConstants.BIGINT_TYPE_NAME, UDFToLong.class, false, UDFToLong.class.getSimpleName());
        system.registerUDF(serdeConstants.FLOAT_TYPE_NAME, UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
        system.registerUDF(serdeConstants.DOUBLE_TYPE_NAME, UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
        system.registerUDF(serdeConstants.STRING_TYPE_NAME, UDFToString.class, false, UDFToString.class.getSimpleName());
        // following mapping is to enable UDFName to UDF while generating expression for default value (in operator tree)
        //  e.g. cast(4 as string) is serialized as UDFToString(4) into metastore, to allow us to generate appropriate UDF for
        //  UDFToString we need the following mappings
        // Rest of the types e.g. DATE, CHAR, VARCHAR etc are already registered
        system.registerUDF(UDFToString.class.getSimpleName(), UDFToString.class, false, UDFToString.class.getSimpleName());
        system.registerUDF(UDFToBoolean.class.getSimpleName(), UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
        system.registerUDF(UDFToDouble.class.getSimpleName(), UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
        system.registerUDF(UDFToFloat.class.getSimpleName(), UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
        system.registerUDF(UDFToInteger.class.getSimpleName(), UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
        system.registerUDF(UDFToLong.class.getSimpleName(), UDFToLong.class, false, UDFToLong.class.getSimpleName());
        system.registerUDF(UDFToShort.class.getSimpleName(), UDFToShort.class, false, UDFToShort.class.getSimpleName());
        system.registerUDF(UDFToByte.class.getSimpleName(), UDFToByte.class, false, UDFToByte.class.getSimpleName());

        system.registerGenericUDF(serdeConstants.DATE_TYPE_NAME, GenericUDFToDate.class);
        system.registerGenericUDF(serdeConstants.TIMESTAMP_TYPE_NAME, GenericUDFTimestamp.class);
        system.registerGenericUDF(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, GenericUDFToTimestampLocalTZ.class);
        system.registerGenericUDF(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, GenericUDFToIntervalYearMonth.class);
        system.registerGenericUDF(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, GenericUDFToIntervalDayTime.class);
        system.registerGenericUDF(serdeConstants.BINARY_TYPE_NAME, GenericUDFToBinary.class);
        system.registerGenericUDF(serdeConstants.DECIMAL_TYPE_NAME, GenericUDFToDecimal.class);
        system.registerGenericUDF(serdeConstants.VARCHAR_TYPE_NAME, GenericUDFToVarchar.class);
        system.registerGenericUDF(serdeConstants.CHAR_TYPE_NAME, GenericUDFToChar.class);

        // Generic UDFs
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

        //PTF declarations
        system.registerGenericUDF(LEAD_FUNC_NAME, GenericUDFLead.class);
        system.registerGenericUDF(LAG_FUNC_NAME, GenericUDFLag.class);

        // Arithmetic specializations are done in a convoluted manner; mark them as built-in.
        system.registerHiddenBuiltIn(GenericUDFOPDTIMinus.class);
        system.registerHiddenBuiltIn(GenericUDFOPDTIPlus.class);
        system.registerHiddenBuiltIn(GenericUDFOPNumericMinus.class);
        system.registerHiddenBuiltIn(GenericUDFOPNumericPlus.class);

        // mask UDFs
        system.registerGenericUDF(GenericUDFMask.UDF_NAME, GenericUDFMask.class);
        system.registerGenericUDF(GenericUDFMaskFirstN.UDF_NAME, GenericUDFMaskFirstN.class);
        system.registerGenericUDF(GenericUDFMaskLastN.UDF_NAME, GenericUDFMaskLastN.class);
        system.registerGenericUDF(GenericUDFMaskShowFirstN.UDF_NAME, GenericUDFMaskShowFirstN.class);
        system.registerGenericUDF(GenericUDFMaskShowLastN.UDF_NAME, GenericUDFMaskShowLastN.class);
        system.registerGenericUDF(GenericUDFMaskHash.UDF_NAME, GenericUDFMaskHash.class);
    }

    public static FunctionInfo getFunctionInfo(String functionName) throws SemanticException
    {
        FunctionInfo info = getTemporaryFunctionInfo(functionName);
        return info != null ? info : system.getFunctionInfo(functionName);
    }

    public static FunctionInfo getTemporaryFunctionInfo(String functionName) throws SemanticException
    {
        Registry registry = SessionState.getRegistry();
        return registry == null ? null : registry.getFunctionInfo(functionName);
    }

    private FunctionRegistry()
    {
        // prevent instantiation
    }
}
