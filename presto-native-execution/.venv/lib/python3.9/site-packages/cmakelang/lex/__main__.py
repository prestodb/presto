from __future__ import print_function

from cmakelang import lex


def main():
  """
  Dump tokenized listfile to stdout for debugging.
  """
  import argparse
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('infile')
  args = parser.parse_args()
  with open(args.infile, 'r') as infile:
    tokens = lex.tokenize(infile.read())
  print('\n'.join(str(x) for x in tokens))


if __name__ == '__main__':
  main()
