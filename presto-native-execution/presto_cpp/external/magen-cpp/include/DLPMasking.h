/*
 * IBM Confidential
 *
 * OCO Source Materials
 *
 * (C) Copyright IBM Corp. 2020, 2022. All Rights Reserved.
 *
 * The source code for this program is not published or otherwise divested of its trade
 * secrets irrespective of what has been deposited with the U.S. Copyright Office.
 *
 * US Government Users Restricted Rights - Use, duplication or disclosure restricted by
 * GSA ADP Schedule Contract with IBM Corp.
 */
#pragma once

#include <iostream>
#include <string>

enum MaskType {
    REDACT = 0, // maskParams contains value to redact to. All datatypes other than string, redacted to default value.   

    REDACT_PRESERVE_LENGTH = 1, // ONLY for strings. First character in maskParams is the redaction character.
                                // Masked output is redaction character repeated as per length of input value.
                                // All other datatypes, redaction to default value occurs.

    SUBSTITUTE = 2, // For strings, secureHash(SHA-256) and base64 encode. All other data-types, obfuscate within the data-type range.
                    // seed paramter: contains seed for repeatability of output. If not provided, redaction to default value happens.

    PARTIAL_REDACT = 3 , // ONLY for strings. maskParams contains Regex or string slice expressions to apply to input.

    FORMAT_PRESERVING = 4, // Obfuscation with input validation. If input not valid according to format, redaction to default value happens.
                           // format parameter: contains the format to use. If unknown to system, Identifier Masking(7) happens.
                           // seed paramter: contains seed for repeatability of output. If empty/null, random seed is used.

    DATE_AGING = 5, // ONLY for date/datetime. "days=+/-" named parameter in maskParams controls how date is aged.

    DATE_PRESERVE_PERIOD = 6, // Unused.

    IDENTIFIER = 7, // Alphabets and digits are masked. All other characters remain as-is.
                    // seed paramter: contains seed for repeatability of output. If empty/null, random seed is used.

    FORMAT_PRESERVING_NOISE = 8, // Unused

    FORMAT_PRESERVING_PARTIAL = 9, // Only for Email currently.
                                   //  maskParams contains Regex and format names for part validations and generation.

    FORMAT_PRESERVING_FABRICATION = 10, // Obfuscation without input validation.
                                        // format parameter: contains the format to use. If unknown to system, Identifier Masking(7) happens.
                                        // seed paramter: contains seed for repeatability of output. If empty/null, random seed is used.

    NUMERIC_SHIFT = 11, // Only for numerical data-types. maskParams contain integer value used as a % to shift input value by.

    MASKTYPE_END
};
//MASKTYPE_END added to limit the enum value, do not assign any value to it.

enum DataBaseType {
    DB2 = 0,
    OTHERS
};

/**
 * @brief Mask a string/char datatype input
 *  
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param outDataMaxLen max length of column value
 * @param outData where to write masked output. Should be allocated already, the size of which  
 *                should be outDataMaxLen + 1. This is for null termination i.e '\0' at the end of the string.
 *        Guaranteed to always contain a masked output and never unmasked data.
 * @param nullIndicator whether input is null (-1)
 */
void mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, const char* inData, int outDataMaxLen,
        char *outData, const int* nullIndicator);

/**
 * @brief Mask a integer datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return int maskedValue. Guaranteed to always contain a masked output.
 */
int mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, int inData,
        const int* nullIndicator);

/**
 * @brief Mask a short datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return short maskedValue. Guaranteed to always contain a masked output
 */
short mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, short inData,
        const int* nullIndicator);

/**
 * @brief Mask a long datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return long maskedValue. Guaranteed to always contain a masked output
 */
long mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, long inData,
        const int* nullIndicator);

/**
 * @brief Mask a float datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return float maskedValue. Guaranteed to always contain a masked output
 */
float mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, float inData,
        const int* nullIndicator);
        
/**
 * @brief Mask a double datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return double maskedValue. Guaranteed to always contain a masked output.
 */
double mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, double inData,
        const int* nullIndicator);

/**
 * @brief Mask a bool datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return bool maskedValue. Guaranteed to always contain a masked output
 */
bool mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, bool inData,
        const int* nullIndicator);

/**
 * @brief Mask a long long datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param nullIndicator whether input is null (-1)
 * @return longlong maskedValue. Guaranteed to always contain a masked output
 */
long long mask(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, const long long& inData,
        const int* nullIndicator);

/**
 * @brief Mask a db specific numeric/decimal datatype input
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param precision of inputData
 * @param scale of inputData
 * @param outDataMaxLen max length of output value
 * @param outData where to write masked output. Should be allocated already, the size of which  
 *                should be outDataMaxLen + 1. This is for null termination i.e '\0' at the end of the string.
 *        Guaranteed to always contain a masked output and never unmasked data.
 * @param nullIndicator whether input is null (-1)
 * @param DataBaseType @see DataBaseType
 * @return short - 0 indicates success, any other value means failure to mask.
 */
short maskNumerics(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, const char* inData, 
        const int* precision, const int* scale, int outDataMaxLen,
        char *outData, const int* nullIndicator, enum DataBaseType dbTypeName);


/*
 *  Mask a Date input.
 *
 *  Note: Some DB's native C type for date is char[n] in ISO Date format (YYYY-mm-dd).
 *  If not, use DB's built_in function or DB's C API's to convert type to 
 *  ISO Date char format before calling this function.
 *  Returned value is also in ISO Date char format.
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports.
 *        Can also contain `same=week|month|quarter|year` for same period masking.
 *                         `minDate=<>;maxDate=<>` to control range within which to obfuscate.
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param outDataMaxLen max length of output value
 * @param outData where to write masked output. Should be allocated already, the size of which  
 *                should be outDataMaxLen + 1. This is for null termination i.e '\0' at the end of the string.
 *        Guaranteed to always contain a masked output and never unmasked data.
 * @param nullIndicator whether input is null (-1)
 * @return short - 0 indicates success, any other value means failure to mask.
 */
short maskDate(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, const char* inDate,
        int outDataMaxLen, char *outData, const int* nullIndicator);


/*
 * Mask a Time input. Only supports REDACT. Value to redact to is specified
 * in the maskParams parameter.
 * 
 * Note: Some DB's native C type for time is char[n] in (hh.mm.ss.nnn).
 *  This api expects a ISO time char format i.e hh:mm:ss.nnn
 *  Use DB's built_in function or DB's C API's to convert type to 
 *  ISO time char format before calling this function.
 *  Returned value is also in ISO time char format.
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports.
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param outDataMaxLen max length of output value
 * @param outData where to write masked output. Should be allocated already, the size of which  
 *                should be outDataMaxLen + 1. This is for null termination i.e '\0' at the end of the string.
 *        Guaranteed to always contain a masked output and never unmasked data.
 * @param nullIndicator whether input is null (-1)
 * @return short - 0 indicates success, any other value means failure to mask.
 *
 */
short maskTime(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, const char* inTime,
        int outDataMaxLen, char *outData, const int* nullIndicator);


/*
 * Mask a DateTime input.
 * 
 *  NOTE: Some DB's native C type for datetime/timestamp is char[n] 
 *  in (YYYY-MM-DD-hh.mm.ss.nnn).
 *  This api expects a ISO Datetime char format i.e YYYY-MM-DD hh:mm:ss.nnn
 *  Use DB's built_in function or DB's C API's to convert type to 
 *  ISO Datetime char format before calling this function.
 *  Returned value is also in ISO Datetime char format.
 * 
 * @param maskType @see MaskType
 * @param maskParams parameters to specific maskType. @see MaskType definition of what each maskType supports
 *         Can also contain `same=week|month|quarter|year` for same period masking.
 *                         `minDate=<>;maxDate=<>` to control range within which to obfuscate.
 * @param format Format Name for obfuscation maskTypes. 
 * @param seed initializationVector that controls repeatability of substitution/obfuscation mask types.
 * @param inData inputData to mask
 * @param outDataMaxLen max length of output value
 * @param outData where to write masked output. Should be allocated already, the size of which  
 *                should be outDataMaxLen + 1. This is for null termination i.e '\0' at the end of the string.
 *                Guaranteed to always contain a masked output and never unmasked data.
 * @param nullIndicator whether input is null (-1)
 * @return short - 0 indicates success, any other value means failure to mask.
 */
short maskDatetime(enum MaskType maskType, const char* maskParams,
        const char* format, const char* seed, const char* inDatetime,
        int outDataMaxLen, char *outData, const int* nullIndicator);