from cmakelang.parse.additional_nodes import TupleParser
from cmakelang.parse.argument_nodes import StandardArgTree


def parse_set_target_properties(ctx, tokens, breakstack):
  """
  ::
    set_target_properties(target1 target2 ...
                        PROPERTIES prop1 value1
                        prop2 value2 ...)
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='+',
      kwargs={
          "PROPERTIES": TupleParser(2, '+', [])
      },
      flags=[],
      breakstack=breakstack)


def populate_db(parse_db):
  parse_db["set_target_properties"] = parse_set_target_properties
