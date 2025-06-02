from __future__ import unicode_literals


class EnumObject(object):
  """
  Simple enumeration base. Design inspired by clang python bindings
  BaseEnumeration. Subclasses must provide class member _id_map.
  """
  _id_map = {}

  @classmethod
  def register_value(cls, value, obj):
    assert cls._id_map is not EnumObject._id_map

    # If there is already an object with this value then raise an exception
    if value in cls._id_map:
      raise ValueError('{0} value {1} already loaded'.format(
          cls.__name__, value))

    cls._id_map[value] = obj

  @classmethod
  def from_id(cls, qid):
    if qid in cls._id_map:
      return cls._id_map[qid]

    raise ValueError("{} is not a valid {} value".format(qid, cls.__name__))

  @classmethod
  def assign_names(cls):
    for key, value in vars(cls).items():
      if isinstance(value, cls):
        value.name_ = key

  @classmethod
  def from_name(cls, name):
    if hasattr(cls, name):
      obj = getattr(cls, name)
      if isinstance(obj, cls):
        return obj
    raise ValueError("{} is not a valid {} enum".format(name, cls.__name__))

  @classmethod
  def get(cls, name, default=None):
    if hasattr(cls, name):
      obj = getattr(cls, name)
      if isinstance(obj, cls):
        return obj

      raise ValueError("{} is not a valid {} enum".format(name, cls.__name__))
    return default

  def __init__(self, value):
    self.value = value
    self.name_ = None
    self.__class__.register_value(value, self)

  @property
  def name(self):
    """Get the enumeration name of this value."""
    if self.name_ is None:
      self.__class__.assign_names()
    return self.name_

  def as_dict(self):
    return self.name

  def __repr__(self):
    return '{}.{}'.format(self.__class__.__name__, self.name)


def stable_wrap(wrapper, paragraph_text):
  """
  textwrap doesn't appear to be stable. We run it multiple times until it
  converges
  """

  history = [paragraph_text]
  prev_text = paragraph_text
  wrap_text = paragraph_text
  for _ in range(8):
    lines = wrapper.wrap(wrap_text)
    next_text = '\n'.join(line for line in lines)
    if next_text == prev_text:
      return lines
    prev_text = next_text
    history.append(next_text)

    lines[0] = lines[0][len(wrapper.initial_indent):]
    lines[1:] = [line[len(wrapper.subsequent_indent):] for line in lines[1:]]
    wrap_text = '\n'.join(lines)

  assert False, ("textwrap failed to converge on:\n\n {}"
                 .format('\n\n'.join(history)))
  return []


class UserError(Exception):
  """Raised when we encounter a fatal problem with usage: e.g. parse error,
     config error, input error, etc."""

  def __init__(self, msg=None):
    super(UserError, self).__init__()
    self.msg = msg

  def __repr__(self):
    return self.msg


class InternalError(Exception):
  """Raised when we encounter something we do not expect, indicating a problem
     with the code itself."""

  def __init__(self, msg=None):
    super(InternalError, self).__init__()
    self.msg = msg

  def __repr__(self):
    return self.msg


class FormatError(Exception):
  """Raised during format or format --check indicating that the program should
     exit with nonzero status code."""

  def __init__(self, msg=None):
    super(FormatError, self).__init__()
    self.msg = msg

  def __repr__(self):
    return self.msg
