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

#include "TypeSignatureVisitor.h"

#include "TypeSignatureParser.h"

using namespace antlrcpp;
using namespace facebook::presto::type;
using namespace antlr4;

TypeSignatureParser::TypeSignatureParser(TokenStream* input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(
      this, _atn, _decisionToDFA, _sharedContextCache);
}

TypeSignatureParser::~TypeSignatureParser() {
  delete _interpreter;
}

std::string TypeSignatureParser::getGrammarFileName() const {
  return "TypeSignature.g4";
}

const std::vector<std::string>& TypeSignatureParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& TypeSignatureParser::getVocabulary() const {
  return _vocabulary;
}

//----------------- StartContext
//------------------------------------------------------------------

TypeSignatureParser::StartContext::StartContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::Type_specContext*
TypeSignatureParser::StartContext::type_spec() {
  return getRuleContext<TypeSignatureParser::Type_specContext>(0);
}

tree::TerminalNode* TypeSignatureParser::StartContext::EOF() {
  return getToken(TypeSignatureParser::EOF, 0);
}

size_t TypeSignatureParser::StartContext::getRuleIndex() const {
  return TypeSignatureParser::RuleStart;
}

antlrcpp::Any TypeSignatureParser::StartContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitStart(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::StartContext* TypeSignatureParser::start() {
  StartContext* _localctx =
      _tracker.createInstance<StartContext>(_ctx, getState());
  enterRule(_localctx, 0, TypeSignatureParser::RuleStart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(24);
    type_spec();
    setState(25);
    match(TypeSignatureParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Type_specContext
//------------------------------------------------------------------

TypeSignatureParser::Type_specContext::Type_specContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::Named_typeContext*
TypeSignatureParser::Type_specContext::named_type() {
  return getRuleContext<TypeSignatureParser::Named_typeContext>(0);
}

TypeSignatureParser::TypeContext*
TypeSignatureParser::Type_specContext::type() {
  return getRuleContext<TypeSignatureParser::TypeContext>(0);
}

size_t TypeSignatureParser::Type_specContext::getRuleIndex() const {
  return TypeSignatureParser::RuleType_spec;
}

antlrcpp::Any TypeSignatureParser::Type_specContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitType_spec(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Type_specContext* TypeSignatureParser::type_spec() {
  Type_specContext* _localctx =
      _tracker.createInstance<Type_specContext>(_ctx, getState());
  enterRule(_localctx, 2, TypeSignatureParser::RuleType_spec);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(29);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 0, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(27);
        named_type();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(28);
        type();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Named_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Named_typeContext::Named_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::IdentifierContext*
TypeSignatureParser::Named_typeContext::identifier() {
  return getRuleContext<TypeSignatureParser::IdentifierContext>(0);
}

TypeSignatureParser::TypeContext*
TypeSignatureParser::Named_typeContext::type() {
  return getRuleContext<TypeSignatureParser::TypeContext>(0);
}

size_t TypeSignatureParser::Named_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleNamed_type;
}

antlrcpp::Any TypeSignatureParser::Named_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitNamed_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Named_typeContext* TypeSignatureParser::named_type() {
  Named_typeContext* _localctx =
      _tracker.createInstance<Named_typeContext>(_ctx, getState());
  enterRule(_localctx, 4, TypeSignatureParser::RuleNamed_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(31);
    identifier();
    setState(32);
    type();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TypeContext
//------------------------------------------------------------------

TypeSignatureParser::TypeContext::TypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

TypeSignatureParser::Simple_typeContext*
TypeSignatureParser::TypeContext::simple_type() {
  return getRuleContext<TypeSignatureParser::Simple_typeContext>(0);
}

TypeSignatureParser::Decimal_typeContext*
TypeSignatureParser::TypeContext::decimal_type() {
  return getRuleContext<TypeSignatureParser::Decimal_typeContext>(0);
}

TypeSignatureParser::Variable_typeContext*
TypeSignatureParser::TypeContext::variable_type() {
  return getRuleContext<TypeSignatureParser::Variable_typeContext>(0);
}

TypeSignatureParser::Array_typeContext*
TypeSignatureParser::TypeContext::array_type() {
  return getRuleContext<TypeSignatureParser::Array_typeContext>(0);
}

TypeSignatureParser::Map_typeContext*
TypeSignatureParser::TypeContext::map_type() {
  return getRuleContext<TypeSignatureParser::Map_typeContext>(0);
}

TypeSignatureParser::Row_typeContext*
TypeSignatureParser::TypeContext::row_type() {
  return getRuleContext<TypeSignatureParser::Row_typeContext>(0);
}

size_t TypeSignatureParser::TypeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleType;
}

antlrcpp::Any TypeSignatureParser::TypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitType(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::TypeContext* TypeSignatureParser::type() {
  TypeContext* _localctx =
      _tracker.createInstance<TypeContext>(_ctx, getState());
  enterRule(_localctx, 6, TypeSignatureParser::RuleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(40);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 1, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(34);
        simple_type();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(35);
        decimal_type();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(36);
        variable_type();
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(37);
        array_type();
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(38);
        map_type();
        break;
      }

      case 6: {
        enterOuterAlt(_localctx, 6);
        setState(39);
        row_type();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Simple_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Simple_typeContext::Simple_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Simple_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

tree::TerminalNode*
TypeSignatureParser::Simple_typeContext::TYPE_WITH_SPACES() {
  return getToken(TypeSignatureParser::TYPE_WITH_SPACES, 0);
}

size_t TypeSignatureParser::Simple_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleSimple_type;
}

antlrcpp::Any TypeSignatureParser::Simple_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitSimple_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Simple_typeContext* TypeSignatureParser::simple_type() {
  Simple_typeContext* _localctx =
      _tracker.createInstance<Simple_typeContext>(_ctx, getState());
  enterRule(_localctx, 8, TypeSignatureParser::RuleSimple_type);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(42);
    _la = _input->LA(1);
    if (!(_la == TypeSignatureParser::TYPE_WITH_SPACES

          || _la == TypeSignatureParser::WORD)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Variable_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Variable_typeContext::Variable_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Variable_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

std::vector<tree::TerminalNode*>
TypeSignatureParser::Variable_typeContext::NUMBER() {
  return getTokens(TypeSignatureParser::NUMBER);
}

tree::TerminalNode* TypeSignatureParser::Variable_typeContext::NUMBER(
    size_t i) {
  return getToken(TypeSignatureParser::NUMBER, i);
}

size_t TypeSignatureParser::Variable_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleVariable_type;
}

antlrcpp::Any TypeSignatureParser::Variable_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitVariable_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Variable_typeContext*
TypeSignatureParser::variable_type() {
  Variable_typeContext* _localctx =
      _tracker.createInstance<Variable_typeContext>(_ctx, getState());
  enterRule(_localctx, 10, TypeSignatureParser::RuleVariable_type);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(56);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 3, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(44);

        if (!(isVarToken()))
          throw FailedPredicateException(this, " isVarToken() ");
        setState(45);
        match(TypeSignatureParser::WORD);
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(46);

        if (!(isVarToken()))
          throw FailedPredicateException(this, " isVarToken() ");
        setState(47);
        match(TypeSignatureParser::WORD);
        setState(48);
        match(TypeSignatureParser::T__0);
        setState(52);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == TypeSignatureParser::NUMBER) {
          setState(49);
          match(TypeSignatureParser::NUMBER);
          setState(54);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(55);
        match(TypeSignatureParser::T__1);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Decimal_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Decimal_typeContext::Decimal_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Decimal_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

std::vector<tree::TerminalNode*>
TypeSignatureParser::Decimal_typeContext::NUMBER() {
  return getTokens(TypeSignatureParser::NUMBER);
}

tree::TerminalNode* TypeSignatureParser::Decimal_typeContext::NUMBER(size_t i) {
  return getToken(TypeSignatureParser::NUMBER, i);
}

size_t TypeSignatureParser::Decimal_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleDecimal_type;
}

antlrcpp::Any TypeSignatureParser::Decimal_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitDecimal_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Decimal_typeContext* TypeSignatureParser::decimal_type() {
  Decimal_typeContext* _localctx =
      _tracker.createInstance<Decimal_typeContext>(_ctx, getState());
  enterRule(_localctx, 12, TypeSignatureParser::RuleDecimal_type);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(58);

    if (!(isDecimalToken()))
      throw FailedPredicateException(this, " isDecimalToken() ");
    setState(59);
    match(TypeSignatureParser::WORD);
    setState(60);
    match(TypeSignatureParser::T__0);
    setState(64);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TypeSignatureParser::NUMBER) {
      setState(61);
      match(TypeSignatureParser::NUMBER);
      setState(66);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(67);
    match(TypeSignatureParser::T__2);
    setState(71);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TypeSignatureParser::NUMBER) {
      setState(68);
      match(TypeSignatureParser::NUMBER);
      setState(73);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(74);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Type_listContext
//------------------------------------------------------------------

TypeSignatureParser::Type_listContext::Type_listContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<TypeSignatureParser::Type_specContext*>
TypeSignatureParser::Type_listContext::type_spec() {
  return getRuleContexts<TypeSignatureParser::Type_specContext>();
}

TypeSignatureParser::Type_specContext*
TypeSignatureParser::Type_listContext::type_spec(size_t i) {
  return getRuleContext<TypeSignatureParser::Type_specContext>(i);
}

size_t TypeSignatureParser::Type_listContext::getRuleIndex() const {
  return TypeSignatureParser::RuleType_list;
}

antlrcpp::Any TypeSignatureParser::Type_listContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitType_list(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Type_listContext* TypeSignatureParser::type_list() {
  Type_listContext* _localctx =
      _tracker.createInstance<Type_listContext>(_ctx, getState());
  enterRule(_localctx, 14, TypeSignatureParser::RuleType_list);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(76);
    type_spec();
    setState(81);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TypeSignatureParser::T__2) {
      setState(77);
      match(TypeSignatureParser::T__2);
      setState(78);
      type_spec();
      setState(83);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Row_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Row_typeContext::Row_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Row_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

TypeSignatureParser::Type_listContext*
TypeSignatureParser::Row_typeContext::type_list() {
  return getRuleContext<TypeSignatureParser::Type_listContext>(0);
}

size_t TypeSignatureParser::Row_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleRow_type;
}

antlrcpp::Any TypeSignatureParser::Row_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitRow_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Row_typeContext* TypeSignatureParser::row_type() {
  Row_typeContext* _localctx =
      _tracker.createInstance<Row_typeContext>(_ctx, getState());
  enterRule(_localctx, 16, TypeSignatureParser::RuleRow_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(84);

    if (!(isRowToken()))
      throw FailedPredicateException(this, " isRowToken() ");
    setState(85);
    match(TypeSignatureParser::WORD);
    setState(86);
    match(TypeSignatureParser::T__0);
    setState(87);
    type_list();
    setState(88);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Map_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Map_typeContext::Map_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Map_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

std::vector<TypeSignatureParser::TypeContext*>
TypeSignatureParser::Map_typeContext::type() {
  return getRuleContexts<TypeSignatureParser::TypeContext>();
}

TypeSignatureParser::TypeContext* TypeSignatureParser::Map_typeContext::type(
    size_t i) {
  return getRuleContext<TypeSignatureParser::TypeContext>(i);
}

size_t TypeSignatureParser::Map_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleMap_type;
}

antlrcpp::Any TypeSignatureParser::Map_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitMap_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Map_typeContext* TypeSignatureParser::map_type() {
  Map_typeContext* _localctx =
      _tracker.createInstance<Map_typeContext>(_ctx, getState());
  enterRule(_localctx, 18, TypeSignatureParser::RuleMap_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(90);

    if (!(isMapToken()))
      throw FailedPredicateException(this, " isMapToken() ");
    setState(91);
    match(TypeSignatureParser::WORD);
    setState(92);
    match(TypeSignatureParser::T__0);
    setState(93);
    type();
    setState(94);
    match(TypeSignatureParser::T__2);
    setState(95);
    type();
    setState(96);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Array_typeContext
//------------------------------------------------------------------

TypeSignatureParser::Array_typeContext::Array_typeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::Array_typeContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

TypeSignatureParser::TypeContext*
TypeSignatureParser::Array_typeContext::type() {
  return getRuleContext<TypeSignatureParser::TypeContext>(0);
}

size_t TypeSignatureParser::Array_typeContext::getRuleIndex() const {
  return TypeSignatureParser::RuleArray_type;
}

antlrcpp::Any TypeSignatureParser::Array_typeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitArray_type(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::Array_typeContext* TypeSignatureParser::array_type() {
  Array_typeContext* _localctx =
      _tracker.createInstance<Array_typeContext>(_ctx, getState());
  enterRule(_localctx, 20, TypeSignatureParser::RuleArray_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(98);

    if (!(isArrayToken()))
      throw FailedPredicateException(this, " isArrayToken() ");
    setState(99);
    match(TypeSignatureParser::WORD);
    setState(100);
    match(TypeSignatureParser::T__0);
    setState(101);
    type();
    setState(102);
    match(TypeSignatureParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext
//------------------------------------------------------------------

TypeSignatureParser::IdentifierContext::IdentifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* TypeSignatureParser::IdentifierContext::QUOTED_ID() {
  return getToken(TypeSignatureParser::QUOTED_ID, 0);
}

tree::TerminalNode* TypeSignatureParser::IdentifierContext::WORD() {
  return getToken(TypeSignatureParser::WORD, 0);
}

size_t TypeSignatureParser::IdentifierContext::getRuleIndex() const {
  return TypeSignatureParser::RuleIdentifier;
}

antlrcpp::Any TypeSignatureParser::IdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<TypeSignatureVisitor*>(visitor))
    return parserVisitor->visitIdentifier(this);
  else
    return visitor->visitChildren(this);
}

TypeSignatureParser::IdentifierContext* TypeSignatureParser::identifier() {
  IdentifierContext* _localctx =
      _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 22, TypeSignatureParser::RuleIdentifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(104);
    _la = _input->LA(1);
    if (!(_la == TypeSignatureParser::WORD

          || _la == TypeSignatureParser::QUOTED_ID)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool TypeSignatureParser::sempred(
    RuleContext* context,
    size_t ruleIndex,
    size_t predicateIndex) {
  switch (ruleIndex) {
    case 5:
      return variable_typeSempred(
          antlrcpp::downCast<Variable_typeContext*>(context), predicateIndex);
    case 6:
      return decimal_typeSempred(
          antlrcpp::downCast<Decimal_typeContext*>(context), predicateIndex);
    case 8:
      return row_typeSempred(
          antlrcpp::downCast<Row_typeContext*>(context), predicateIndex);
    case 9:
      return map_typeSempred(
          antlrcpp::downCast<Map_typeContext*>(context), predicateIndex);
    case 10:
      return array_typeSempred(
          antlrcpp::downCast<Array_typeContext*>(context), predicateIndex);

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::variable_typeSempred(
    Variable_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 0:
      return isVarToken();
    case 1:
      return isVarToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::decimal_typeSempred(
    Decimal_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 2:
      return isDecimalToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::row_typeSempred(
    Row_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 3:
      return isRowToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::map_typeSempred(
    Map_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 4:
      return isMapToken();

    default:
      break;
  }
  return true;
}

bool TypeSignatureParser::array_typeSempred(
    Array_typeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 5:
      return isArrayToken();

    default:
      break;
  }
  return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> TypeSignatureParser::_decisionToDFA;
atn::PredictionContextCache TypeSignatureParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN TypeSignatureParser::_atn;
std::vector<uint16_t> TypeSignatureParser::_serializedATN;

std::vector<std::string> TypeSignatureParser::_ruleNames = {
    "start",
    "type_spec",
    "named_type",
    "type",
    "simple_type",
    "variable_type",
    "decimal_type",
    "type_list",
    "row_type",
    "map_type",
    "array_type",
    "identifier"};

std::vector<std::string> TypeSignatureParser::_literalNames = {
    "",
    "'('",
    "')'",
    "','"};

std::vector<std::string> TypeSignatureParser::_symbolicNames = {
    "",
    "",
    "",
    "",
    "TYPE_WITH_SPACES",
    "WORD",
    "QUOTED_ID",
    "NUMBER",
    "WHITESPACE"};

dfa::Vocabulary TypeSignatureParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> TypeSignatureParser::_tokenNames;

TypeSignatureParser::Initializer::Initializer() {
  for (size_t i = 0; i < _symbolicNames.size(); ++i) {
    std::string name = _vocabulary.getLiteralName(i);
    if (name.empty()) {
      name = _vocabulary.getSymbolicName(i);
    }

    if (name.empty()) {
      _tokenNames.push_back("<INVALID>");
    } else {
      _tokenNames.push_back(name);
    }
  }

  static const uint16_t serializedATNSegment0[] = {
      0x3,  0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964,
      0x3,  0xa,    0x6d,   0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
      0x9,  0x3,    0x4,    0x4,    0x9,    0x4,    0x4,    0x5,    0x9,
      0x5,  0x4,    0x6,    0x9,    0x6,    0x4,    0x7,    0x9,    0x7,
      0x4,  0x8,    0x9,    0x8,    0x4,    0x9,    0x9,    0x9,    0x4,
      0xa,  0x9,    0xa,    0x4,    0xb,    0x9,    0xb,    0x4,    0xc,
      0x9,  0xc,    0x4,    0xd,    0x9,    0xd,    0x3,    0x2,    0x3,
      0x2,  0x3,    0x2,    0x3,    0x3,    0x3,    0x3,    0x5,    0x3,
      0x20, 0xa,    0x3,    0x3,    0x4,    0x3,    0x4,    0x3,    0x4,
      0x3,  0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,  0x3,    0x5,    0x5,    0x5,    0x2b,   0xa,    0x5,    0x3,
      0x6,  0x3,    0x6,    0x3,    0x7,    0x3,    0x7,    0x3,    0x7,
      0x3,  0x7,    0x3,    0x7,    0x3,    0x7,    0x7,    0x7,    0x35,
      0xa,  0x7,    0xc,    0x7,    0xe,    0x7,    0x38,   0xb,    0x7,
      0x3,  0x7,    0x5,    0x7,    0x3b,   0xa,    0x7,    0x3,    0x8,
      0x3,  0x8,    0x3,    0x8,    0x3,    0x8,    0x7,    0x8,    0x41,
      0xa,  0x8,    0xc,    0x8,    0xe,    0x8,    0x44,   0xb,    0x8,
      0x3,  0x8,    0x3,    0x8,    0x7,    0x8,    0x48,   0xa,    0x8,
      0xc,  0x8,    0xe,    0x8,    0x4b,   0xb,    0x8,    0x3,    0x8,
      0x3,  0x8,    0x3,    0x9,    0x3,    0x9,    0x3,    0x9,    0x7,
      0x9,  0x52,   0xa,    0x9,    0xc,    0x9,    0xe,    0x9,    0x55,
      0xb,  0x9,    0x3,    0xa,    0x3,    0xa,    0x3,    0xa,    0x3,
      0xa,  0x3,    0xa,    0x3,    0xa,    0x3,    0xb,    0x3,    0xb,
      0x3,  0xb,    0x3,    0xb,    0x3,    0xb,    0x3,    0xb,    0x3,
      0xb,  0x3,    0xb,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x3,  0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xd,    0x3,
      0xd,  0x3,    0xd,    0x2,    0x2,    0xe,    0x2,    0x4,    0x6,
      0x8,  0xa,    0xc,    0xe,    0x10,   0x12,   0x14,   0x16,   0x18,
      0x2,  0x4,    0x3,    0x2,    0x6,    0x7,    0x3,    0x2,    0x7,
      0x8,  0x2,    0x6b,   0x2,    0x1a,   0x3,    0x2,    0x2,    0x2,
      0x4,  0x1f,   0x3,    0x2,    0x2,    0x2,    0x6,    0x21,   0x3,
      0x2,  0x2,    0x2,    0x8,    0x2a,   0x3,    0x2,    0x2,    0x2,
      0xa,  0x2c,   0x3,    0x2,    0x2,    0x2,    0xc,    0x3a,   0x3,
      0x2,  0x2,    0x2,    0xe,    0x3c,   0x3,    0x2,    0x2,    0x2,
      0x10, 0x4e,   0x3,    0x2,    0x2,    0x2,    0x12,   0x56,   0x3,
      0x2,  0x2,    0x2,    0x14,   0x5c,   0x3,    0x2,    0x2,    0x2,
      0x16, 0x64,   0x3,    0x2,    0x2,    0x2,    0x18,   0x6a,   0x3,
      0x2,  0x2,    0x2,    0x1a,   0x1b,   0x5,    0x4,    0x3,    0x2,
      0x1b, 0x1c,   0x7,    0x2,    0x2,    0x3,    0x1c,   0x3,    0x3,
      0x2,  0x2,    0x2,    0x1d,   0x20,   0x5,    0x6,    0x4,    0x2,
      0x1e, 0x20,   0x5,    0x8,    0x5,    0x2,    0x1f,   0x1d,   0x3,
      0x2,  0x2,    0x2,    0x1f,   0x1e,   0x3,    0x2,    0x2,    0x2,
      0x20, 0x5,    0x3,    0x2,    0x2,    0x2,    0x21,   0x22,   0x5,
      0x18, 0xd,    0x2,    0x22,   0x23,   0x5,    0x8,    0x5,    0x2,
      0x23, 0x7,    0x3,    0x2,    0x2,    0x2,    0x24,   0x2b,   0x5,
      0xa,  0x6,    0x2,    0x25,   0x2b,   0x5,    0xe,    0x8,    0x2,
      0x26, 0x2b,   0x5,    0xc,    0x7,    0x2,    0x27,   0x2b,   0x5,
      0x16, 0xc,    0x2,    0x28,   0x2b,   0x5,    0x14,   0xb,    0x2,
      0x29, 0x2b,   0x5,    0x12,   0xa,    0x2,    0x2a,   0x24,   0x3,
      0x2,  0x2,    0x2,    0x2a,   0x25,   0x3,    0x2,    0x2,    0x2,
      0x2a, 0x26,   0x3,    0x2,    0x2,    0x2,    0x2a,   0x27,   0x3,
      0x2,  0x2,    0x2,    0x2a,   0x28,   0x3,    0x2,    0x2,    0x2,
      0x2a, 0x29,   0x3,    0x2,    0x2,    0x2,    0x2b,   0x9,    0x3,
      0x2,  0x2,    0x2,    0x2c,   0x2d,   0x9,    0x2,    0x2,    0x2,
      0x2d, 0xb,    0x3,    0x2,    0x2,    0x2,    0x2e,   0x2f,   0x6,
      0x7,  0x2,    0x2,    0x2f,   0x3b,   0x7,    0x7,    0x2,    0x2,
      0x30, 0x31,   0x6,    0x7,    0x3,    0x2,    0x31,   0x32,   0x7,
      0x7,  0x2,    0x2,    0x32,   0x36,   0x7,    0x3,    0x2,    0x2,
      0x33, 0x35,   0x7,    0x9,    0x2,    0x2,    0x34,   0x33,   0x3,
      0x2,  0x2,    0x2,    0x35,   0x38,   0x3,    0x2,    0x2,    0x2,
      0x36, 0x34,   0x3,    0x2,    0x2,    0x2,    0x36,   0x37,   0x3,
      0x2,  0x2,    0x2,    0x37,   0x39,   0x3,    0x2,    0x2,    0x2,
      0x38, 0x36,   0x3,    0x2,    0x2,    0x2,    0x39,   0x3b,   0x7,
      0x4,  0x2,    0x2,    0x3a,   0x2e,   0x3,    0x2,    0x2,    0x2,
      0x3a, 0x30,   0x3,    0x2,    0x2,    0x2,    0x3b,   0xd,    0x3,
      0x2,  0x2,    0x2,    0x3c,   0x3d,   0x6,    0x8,    0x4,    0x2,
      0x3d, 0x3e,   0x7,    0x7,    0x2,    0x2,    0x3e,   0x42,   0x7,
      0x3,  0x2,    0x2,    0x3f,   0x41,   0x7,    0x9,    0x2,    0x2,
      0x40, 0x3f,   0x3,    0x2,    0x2,    0x2,    0x41,   0x44,   0x3,
      0x2,  0x2,    0x2,    0x42,   0x40,   0x3,    0x2,    0x2,    0x2,
      0x42, 0x43,   0x3,    0x2,    0x2,    0x2,    0x43,   0x45,   0x3,
      0x2,  0x2,    0x2,    0x44,   0x42,   0x3,    0x2,    0x2,    0x2,
      0x45, 0x49,   0x7,    0x5,    0x2,    0x2,    0x46,   0x48,   0x7,
      0x9,  0x2,    0x2,    0x47,   0x46,   0x3,    0x2,    0x2,    0x2,
      0x48, 0x4b,   0x3,    0x2,    0x2,    0x2,    0x49,   0x47,   0x3,
      0x2,  0x2,    0x2,    0x49,   0x4a,   0x3,    0x2,    0x2,    0x2,
      0x4a, 0x4c,   0x3,    0x2,    0x2,    0x2,    0x4b,   0x49,   0x3,
      0x2,  0x2,    0x2,    0x4c,   0x4d,   0x7,    0x4,    0x2,    0x2,
      0x4d, 0xf,    0x3,    0x2,    0x2,    0x2,    0x4e,   0x53,   0x5,
      0x4,  0x3,    0x2,    0x4f,   0x50,   0x7,    0x5,    0x2,    0x2,
      0x50, 0x52,   0x5,    0x4,    0x3,    0x2,    0x51,   0x4f,   0x3,
      0x2,  0x2,    0x2,    0x52,   0x55,   0x3,    0x2,    0x2,    0x2,
      0x53, 0x51,   0x3,    0x2,    0x2,    0x2,    0x53,   0x54,   0x3,
      0x2,  0x2,    0x2,    0x54,   0x11,   0x3,    0x2,    0x2,    0x2,
      0x55, 0x53,   0x3,    0x2,    0x2,    0x2,    0x56,   0x57,   0x6,
      0xa,  0x5,    0x2,    0x57,   0x58,   0x7,    0x7,    0x2,    0x2,
      0x58, 0x59,   0x7,    0x3,    0x2,    0x2,    0x59,   0x5a,   0x5,
      0x10, 0x9,    0x2,    0x5a,   0x5b,   0x7,    0x4,    0x2,    0x2,
      0x5b, 0x13,   0x3,    0x2,    0x2,    0x2,    0x5c,   0x5d,   0x6,
      0xb,  0x6,    0x2,    0x5d,   0x5e,   0x7,    0x7,    0x2,    0x2,
      0x5e, 0x5f,   0x7,    0x3,    0x2,    0x2,    0x5f,   0x60,   0x5,
      0x8,  0x5,    0x2,    0x60,   0x61,   0x7,    0x5,    0x2,    0x2,
      0x61, 0x62,   0x5,    0x8,    0x5,    0x2,    0x62,   0x63,   0x7,
      0x4,  0x2,    0x2,    0x63,   0x15,   0x3,    0x2,    0x2,    0x2,
      0x64, 0x65,   0x6,    0xc,    0x7,    0x2,    0x65,   0x66,   0x7,
      0x7,  0x2,    0x2,    0x66,   0x67,   0x7,    0x3,    0x2,    0x2,
      0x67, 0x68,   0x5,    0x8,    0x5,    0x2,    0x68,   0x69,   0x7,
      0x4,  0x2,    0x2,    0x69,   0x17,   0x3,    0x2,    0x2,    0x2,
      0x6a, 0x6b,   0x9,    0x3,    0x2,    0x2,    0x6b,   0x19,   0x3,
      0x2,  0x2,    0x2,    0x9,    0x1f,   0x2a,   0x36,   0x3a,   0x42,
      0x49, 0x53,
  };

  _serializedATN.insert(
      _serializedATN.end(),
      serializedATNSegment0,
      serializedATNSegment0 +
          sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

TypeSignatureParser::Initializer TypeSignatureParser::_init;
