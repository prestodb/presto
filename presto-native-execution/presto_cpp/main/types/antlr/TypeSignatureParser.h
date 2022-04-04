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

#include <boost/algorithm/string.hpp>
#include "presto_cpp/main/types/TypeSignatureTypeConverter.h"

// Generated from TypeSignature.g4 by ANTLR 4.9.3

#pragma once

#include "antlr4-runtime.h"

namespace facebook::presto::type {

class TypeSignatureParser : public antlr4::Parser {
 public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    TYPE_WITH_SPACES = 4,
    WORD = 5,
    QUOTED_ID = 6,
    NUMBER = 7,
    WHITESPACE = 8
  };

  enum {
    RuleStart = 0,
    RuleType_spec = 1,
    RuleNamed_type = 2,
    RuleType = 3,
    RuleSimple_type = 4,
    RuleVariable_type = 5,
    RuleType_list = 6,
    RuleRow_type = 7,
    RuleMap_type = 8,
    RuleArray_type = 9,
    RuleIdentifier = 10
  };

  explicit TypeSignatureParser(antlr4::TokenStream* input);
  ~TypeSignatureParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override {
    return _atn;
  };
  virtual const std::vector<std::string>& getTokenNames() const override {
    return _tokenNames;
  }; // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  class StartContext;
  class Type_specContext;
  class Named_typeContext;
  class TypeContext;
  class Simple_typeContext;
  class Variable_typeContext;
  class Type_listContext;
  class Row_typeContext;
  class Map_typeContext;
  class Array_typeContext;
  class IdentifierContext;

  class StartContext : public antlr4::ParserRuleContext {
   public:
    StartContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Type_specContext* type_spec();
    antlr4::tree::TerminalNode* EOF();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StartContext* start();

  class Type_specContext : public antlr4::ParserRuleContext {
   public:
    Type_specContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Named_typeContext* named_type();
    TypeContext* type();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Type_specContext* type_spec();

  class Named_typeContext : public antlr4::ParserRuleContext {
   public:
    Named_typeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    TypeContext* type();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Named_typeContext* named_type();

  class TypeContext : public antlr4::ParserRuleContext {
   public:
    TypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Simple_typeContext* simple_type();
    Variable_typeContext* variable_type();
    Array_typeContext* array_type();
    Map_typeContext* map_type();
    Row_typeContext* row_type();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TypeContext* type();

  class Simple_typeContext : public antlr4::ParserRuleContext {
   public:
    Simple_typeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WORD();
    antlr4::tree::TerminalNode* TYPE_WITH_SPACES();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Simple_typeContext* simple_type();

  class Variable_typeContext : public antlr4::ParserRuleContext {
   public:
    Variable_typeContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WORD();
    std::vector<antlr4::tree::TerminalNode*> NUMBER();
    antlr4::tree::TerminalNode* NUMBER(size_t i);

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Variable_typeContext* variable_type();

  class Type_listContext : public antlr4::ParserRuleContext {
   public:
    Type_listContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Type_specContext*> type_spec();
    Type_specContext* type_spec(size_t i);

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Type_listContext* type_list();

  class Row_typeContext : public antlr4::ParserRuleContext {
   public:
    Row_typeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WORD();
    Type_listContext* type_list();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Row_typeContext* row_type();

  class Map_typeContext : public antlr4::ParserRuleContext {
   public:
    Map_typeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WORD();
    std::vector<TypeContext*> type();
    TypeContext* type(size_t i);

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Map_typeContext* map_type();

  class Array_typeContext : public antlr4::ParserRuleContext {
   public:
    Array_typeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WORD();
    TypeContext* type();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Array_typeContext* array_type();

  class IdentifierContext : public antlr4::ParserRuleContext {
   public:
    IdentifierContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* QUOTED_ID();
    antlr4::tree::TerminalNode* WORD();

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierContext* identifier();

  virtual bool sempred(
      antlr4::RuleContext* _localctx,
      size_t ruleIndex,
      size_t predicateIndex) override;
  bool variable_typeSempred(
      Variable_typeContext* _localctx,
      size_t predicateIndex);
  bool row_typeSempred(Row_typeContext* _localctx, size_t predicateIndex);
  bool map_typeSempred(Map_typeContext* _localctx, size_t predicateIndex);
  bool array_typeSempred(Array_typeContext* _localctx, size_t predicateIndex);

 private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;

#define Token() (getCurrentToken()->getText())
#define UpCase(str) (boost::to_upper_copy(str))
#define isRowToken() (UpCase(Token()) == "ROW")
#define isMapToken() (UpCase(Token()) == "MAP")
#define isArrayToken() (UpCase(Token()) == "ARRAY")
#define isVarToken() \
  (UpCase(Token()) == "VARCHAR" || UpCase(Token()) == "VARBINARY")

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

} // namespace facebook::presto::type
