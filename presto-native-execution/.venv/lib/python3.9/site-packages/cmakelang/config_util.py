from __future__ import unicode_literals

import collections
import contextlib
import inspect
import logging
import pprint
import sys
import textwrap

import six

logger = logging.getLogger(__name__)


def serialize(obj, with_help=False, with_defaults=True):
  """
  Return a serializable representation of the object. If the object has an
  `as_dict` method, then it will call and return the output of that method.
  Otherwise return the object itself.
  """
  if hasattr(obj, 'as_odict'):
    fun = getattr(obj, 'as_odict')
    if callable(fun):
      return fun(with_help=with_help, with_defaults=with_defaults)
  if hasattr(obj, 'as_dict'):
    fun = getattr(obj, 'as_dict')
    if callable(fun):
      return fun(with_help=with_help, with_defaults=with_defaults)

  return obj


def parse_bool(string):
  if string.lower() in ('y', 'yes', 't', 'true', '1', 'yup', 'yeah', 'yada'):
    return True
  if string.lower() in ('n', 'no', 'f', 'false', '0', 'nope', 'nah', 'nada'):
    return False

  logger.warning("Ambiguous truthiness of string '%s' evalutes to 'FALSE'",
                 string)
  return False


def get_default(value, default):
  """
  return ``value`` if it is not None, else default
  """
  if value is None:
    return default

  return value


class ExecGlobal(dict):
  """
  We pass this in as the "global" variable when parsing a config. It is
  composed of a nested dictionary and provides methods for mangaging
  the current  "stack" into that nested dictionary
  """

  def __init__(self, filepath):
    super(ExecGlobal, self).__init__()

    # The current stack of push/pop config sections
    self._dict_stack = []

    # This is what we provide for __file__
    self.filepath = filepath

  @contextlib.contextmanager
  def section(self, key):
    self.push_section(key)
    yield None
    self.pop_section()

  def push_section(self, key):
    key = key.replace("-", "_")
    if self._dict_stack:
      stacktop = self._dict_stack[-1]
    else:
      stacktop = self

    if key not in stacktop:
      stacktop[key] = {}
    newtop = stacktop[key]
    self._dict_stack.append(newtop)

  def pop_section(self):
    self._dict_stack.pop(-1)

  def __getitem__(self, key):
    if key in ("_dict_stack", "__name__"):
      # Disallow config script from accessing internal members
      logger.warning("Config illegal attempt to access %s", key)
      return None

    if key == "__file__":
      # Allow config script to access __file__
      return self.filepath

    if hasattr(self, key):
      # Allow config script to access section(), push_section(), pop_section()
      # directly
      return getattr(self, key)

    # Check if any nested scopes contain this name, and return it if they do.
    # this means that sections are "scoped".
    for dictitem in reversed(self._dict_stack):
      if key in dictitem:
        return dictitem[key]

    # Otherwise act like a dictionary
    return super(ExecGlobal, self).__getitem__(key)

  def __setitem__(self, key, value):
    if self._dict_stack:
      self._dict_stack[-1][key] = value
    else:
      super(ExecGlobal, self).__setitem__(key, value)


def _emulate_setname(subclass):
  # pylint: disable=protected-access
  for key, value in subclass.__dict__.items():
    if key.startswith("_"):
      continue

    if hasattr(value, "__set_name__"):
      getattr(value, "__set_name__")(subclass, key)


class ConfigMeta(type):
  """Metaclass that calls __set_name__ on all subclass descriptors for pythons
  prior to 3.6
  """

  def __new__(mcs, name, bases, dct):
    subclass = type.__new__(mcs, name, bases, dct)
    if sys.version_info < (3, 6, 0):
      _emulate_setname(subclass)
    return subclass

  def __setattr__(cls, name, value):
    if hasattr(value, "__set_name__"):
      getattr(value, "__set_name__")(cls, name)


class Descriptor(object):
  # TODO(josh): consider storing a global construction order among
  # descriptors. This would allow us to iterate over config object descriptors
  # in deterministic (construction) order even in pythons where
  # __dict__.items() does not return objects in declaration order.
  pass


class SubtreeDescriptor(Descriptor):
  """Implements the descriptor interface for nested configuration
     objects."""

  def __init__(self, subtree_class):
    super(SubtreeDescriptor, self).__init__()
    self.name = "<unnamed>"
    self.subtree_class = subtree_class

  def get(self, obj):
    if not hasattr(obj, "_" + self.name):
      # Unlike for config variables, for subtree objects we always instanciate
      # the shadow member.
      setattr(obj, "_" + self.name, self.subtree_class())

    return getattr(obj, "_" + self.name)

  def __get__(self, obj, _objtype):
    return self.get(obj)

  def __set__(self, obj, value):
    raise NotImplementedError(
        "Assignment to subtree objects is not supported")

  def __set_name__(self, owner, name):
    # pylint: disable=protected-access
    owner._field_registry.append(self)
    if sys.version_info < (3, 0, 0):
      name = name.decode("utf-8")
    self.name = name

  def consume_value(self, obj, value_dict):
    """Convenience method to consume keyword arguments directly from the
    descriptor interface."""
    if not isinstance(value_dict, dict):
      raise ValueError(
          "value_dict for {}.{} must be a dictionary, not a {}"
          .format(type(obj).__name__, self.name, type(value_dict).__name__))
    self.get(obj).consume_known(value_dict)
    warn_unused(value_dict)

  def legacy_shim_consume(self, obj, kwargs):
    """Consume config variable assignments from the root of the config
    tree. This is the legacy config style and will likely be deprecated
    soon."""
    self.get(obj).legacy_consume(kwargs)

  def add_to_argparser(self, argparser):
    self.subtree_class.add_to_argparser(argparser)


if sys.version_info >= (3, 0, 0):
  VALUE_TYPES = (str, int, float)
else:
  VALUE_TYPES = (str, unicode, int, float)


class FieldDescriptor(Descriptor):
  """Implements the descriptor interface to store metadata (default value,
  docstring, and choices) for each configuration variable.
  """

  def __init__(self, default_value=None, helptext=None, choices=None):
    super(FieldDescriptor, self).__init__()
    self.default_value = default_value
    self.helptext = helptext
    self.choices = choices
    self.name = "<unnamed>"

  def __get__(self, obj, objtype):
    return getattr(obj, "_" + self.name, self.default_value)

  def __set__(self, obj, value):
    setattr(obj, "_" + self.name, value)

  def unset(self, obj):
    if self.has_override(obj):
      delattr(obj, "_" + self.name)

  def __set_name__(self, owner, name):
    # pylint: disable=protected-access
    owner._field_registry.append(self)
    if sys.version_info < (3, 0, 0):
      name = name.decode("utf-8")
    self.name = name

  def consume_value(self, obj, value):
    """Convenience method to consume values directly from the descriptor
    interface."""
    self.__set__(obj, value)

  def has_override(self, obj):
    """Return true if `obj` has an override for this configuration variable."""
    return hasattr(obj, "_" + self.name)

  def add_to_argparse(self, optgroup):
    """Add the config variable as an argument to the command line parser."""
    if self.name == 'additional_commands':
      return

    if isinstance(self.default_value, bool):
      # NOTE(josh): argparse store_true isn't what we want here because we
      # want to distinguish between "not specified" = "default" and
      # "specified"
      optgroup.add_argument(
          '--' + self.name.replace('_', '-'), nargs='?',
          default=None, const=(not self.default_value),
          type=parse_bool, help=self.helptext)
    elif isinstance(self.default_value, VALUE_TYPES):
      optgroup.add_argument(
          '--' + self.name.replace('_', '-'),
          type=type(self.default_value),
          help=self.helptext,
          choices=self.choices)
    elif self.default_value is None:
      # If the value is None then we can't really tell what it's supposed to
      # be. I guess let's assume string in this case.
      optgroup.add_argument(
          '--' + self.name.replace('_', '-'),
          help=self.helptext,
          choices=self.choices)
    # NOTE(josh): argparse behavior is that if the flag is not specified on
    # the command line the value will be None, whereas if it's specified with
    # no arguments then the value will be an empty list. This exactly what we
    # want since we can ignore `None` values.
    elif isinstance(self.default_value, (list, tuple)):
      typearg = None
      if self.default_value:
        typearg = type(self.default_value[0])
      optgroup.add_argument(
          '--' + self.name.replace('_', '-'),
          nargs='*', type=typearg, help=self.helptext)


def serialize_docstring(docstring):
  if "\n" in docstring:
    return docstring.split("\n")
  return docstring


def warn_unused(kwargs):
  unused = []
  for keyword, value in kwargs.items():
    if keyword.startswith("_"):
      continue
    if inspect.ismodule(value):
      continue
    unused.append(keyword)

  if unused:
    logger.warning(
        "The following configuration options were ignored:\n  %s",
        "\n  ".join(unused))


class ConfigObject(six.with_metaclass(ConfigMeta)):
  """
  Base class for encapsulationg options/parameters
  """

  _field_registry = []

  def _update_derived(self):
    """subclass hook to update any derived values after a change."""

  def __init__(self, **kwargs):
    # Ensure that the most derived class has a field registry
    assert hasattr(self.__class__, "_field_registry"), (
        "Derived class {} is missing _field_registry member".format(
            self.__class__.__name__))

    self.consume_known(kwargs)
    self.legacy_consume(kwargs)
    warn_unused(kwargs)

  def consume_known(self, kwargs):
    """Consume known configuration values from a dictionary. Removes the
       values from the dictionary.
    """
    for descr in self._field_registry:
      if descr.name in kwargs:
        descr.consume_value(self, kwargs.pop(descr.name))
    self._update_derived()

  def legacy_consume(self, kwargs):
    """Consume arguments from the root of the configuration dictionary.
    """
    self.consume_known(kwargs)
    for descr in self._field_registry:
      if isinstance(descr, SubtreeDescriptor):
        descr.legacy_shim_consume(self, kwargs)

  @classmethod
  def get_field_names(cls):
    return [descr.name for descr in cls._field_registry]

  def _as_dict(self, dtype, with_help=False, with_defaults=True):
    """
    Return a dictionary mapping field names to their values only for fields
    specified in the constructor
    """
    out = dtype()
    for descr in self._field_registry:
      value = descr.__get__(self, type(self))
      if isinstance(descr, SubtreeDescriptor):
        if not (with_defaults or value.has_override()):
          continue
        if with_help:
          out["_help_" + descr.name] = serialize_docstring(value.__doc__)
        # pylint: disable=protected-access
        out[descr.name] = value._as_dict(dtype, with_help, with_defaults)
      elif isinstance(descr, FieldDescriptor):
        if not (with_defaults or descr.has_override(self)):
          continue
        helptext = descr.helptext
        if helptext and with_help:
          out["_help_" + descr.name] = textwrap.wrap(helptext, 60)
        out[descr.name] = serialize(value)
      else:
        raise RuntimeError("Field registry contains unknown descriptor")
    return out

  def as_dict(self, with_help=False, with_defaults=True):
    return self._as_dict(dict, with_help, with_defaults)

  def as_odict(self, with_help=False, with_defaults=True):
    return self._as_dict(collections.OrderedDict, with_help, with_defaults)

  def has_override(self):
    for descr in self._field_registry:
      if isinstance(descr, SubtreeDescriptor):
        if descr.get(self).has_override():
          return True
      elif isinstance(descr, FieldDescriptor):
        if descr.has_override(self):
          return True
      else:
        raise RuntimeError("Field registry contains unknown descriptor")
    return False

  def dump(self, outfile, depth=0, with_help=True, with_defaults=True):
    indent = "  " * depth
    linewidth = 80 - len(indent)
    comment_linewidth = 80 - len(indent) - len("# ")

    ppr = pprint.PrettyPrinter(indent=2, width=linewidth)
    for descr in self._field_registry:
      fieldname = descr.name
      value = descr.__get__(self, type(self))

      if isinstance(descr, SubtreeDescriptor):
        if not (with_defaults or value.has_override()):
          continue

        if value.__doc__ and with_help:
          section_doc_lines = value.__doc__.split("\n")
          ruler_len = max(len(line) for line in section_doc_lines)
          outfile.write("# {}\n".format("-" * ruler_len))
          for line in section_doc_lines:
            outfile.write(indent)
            outfile.write('# {}\n'.format(line.rstrip()))
          outfile.write("# {}\n".format("-" * ruler_len))

        outfile.write(indent)
        outfile.write("with section(\"{}\"):\n".format(fieldname))
        value.dump(outfile, depth + 1, with_help, with_defaults)
        outfile.write("\n")
      elif isinstance(descr, FieldDescriptor):
        if not (with_defaults or descr.has_override(self)):
          continue

        helptext = descr.helptext
        if helptext and with_help:
          outfile.write("\n")
          for line in textwrap.wrap(helptext, comment_linewidth):
            outfile.write(indent)
            outfile.write('# {}\n'.format(line.rstrip()))

        pretty = "{} = {}".format(fieldname, ppr.pformat(value))
        for line in pretty.split("\n"):
          outfile.write(indent)
          outfile.write(line)
          outfile.write("\n")
      else:
        raise RuntimeError("Field registry contains unknown descriptor")

  @classmethod
  def add_to_argparser(cls, argparser):
    if "\n" in cls.__doc__:
      title, description = cls.__doc__.split("\n", 1)
    else:
      title = cls.__doc__
      description = None

    optgroup = argparser.add_argument_group(
        title=title, description=description)

    for descr in cls._field_registry:
      if isinstance(descr, SubtreeDescriptor):
        descr.add_to_argparser(argparser)
      elif isinstance(descr, FieldDescriptor):
        descr.add_to_argparse(optgroup)

  def clone(self):
    """
    Return a copy of self.
    """
    return self.__class__(**self.as_dict())

  def validate(self):
    # TODO(josh): recurse on sub objects
    # TODO(josh): for any spec item that has choices, make sure the value is
    # in the choices
    # TODO(josh): make sure that the current value is the same type as the
    # default value (unless the default is None)
    return True
