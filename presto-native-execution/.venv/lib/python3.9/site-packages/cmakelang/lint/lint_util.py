from __future__ import unicode_literals
import collections
import logging

from cmakelang.lint import lintdb

logger = logging.getLogger(__name__)


class LintRecord(object):
  """Records an instance of lint at a particular location
  """

  def __init__(self, spec, location, msg):
    self.spec = spec
    self.location = location
    self.msg = msg

  def __repr__(self):
    if self.location is None:
      return " [{:s}] {:s}".format(self.spec.idstr, self.msg)

    if isinstance(self.location, tuple):
      return "{:s}: [{:s}] {:s}".format(
          ",".join("{:02d}".format(val) for val in self.location[:2]),
          self.spec.idstr, self.msg)

    raise ValueError(
        "Unexpected type {} for location".format(type(self.location)))


SuppressionEvent = collections.namedtuple(
    "SuppresionEvent", ["lineno", "mode", "suppressions"])


class FileContext(object):
  def __init__(self, global_ctx, infile_path):
    self.global_ctx = global_ctx
    self.infile_path = infile_path
    self.config = None
    self._lint = []

    # Suppressions active at the current depth
    self._suppressions = set()
    self._supressed_count = collections.defaultdict(int)

    # Map of line number to list of active suppressions starting at this line
    # and ending at the line number of the next entry in the list
    self._suppression_events = []

  def is_idstr(self, idstr):
    return idstr in self.global_ctx.lintdb

  def suppress(self, lineno, idlist):
    """
    Given a list of lint ids, enable a suppression for each one which is not
    already supressed. Return the list of new suppressions
    """
    new_suppressions = []
    for idstr in idlist:
      if idstr in self._suppressions:
        continue
      self._suppressions.add(idstr)
      new_suppressions.append(idstr)

    self._suppression_events.append(
        SuppressionEvent(lineno, "add", list(new_suppressions)))
    return new_suppressions

  def unsuppress(self, lineno, idlist):
    for idstr in idlist:
      if idstr not in self._suppressions:
        logger.warning(
            "Unsupressing %s which is not currently surpressed", idstr)
      self._suppressions.discard(idstr)
    self._suppression_events.append(
        SuppressionEvent(lineno, "remove", list(idlist)))

  def record_lint(self, idstr, *args, **kwargs):
    if idstr in self.config.lint.disabled_codes:
      self._supressed_count[idstr] += 1
      return

    if idstr in self._suppressions:
      self._supressed_count[idstr] += 1
      return

    spec = self.global_ctx.lintdb[idstr]
    location = kwargs.pop("location", ())
    msg = spec.msgfmt.format(*args, **kwargs)
    record = LintRecord(spec, location, msg)
    self._lint.append(record)

  def get_lint(self):
    """Return lint records in sorted order"""
    records = [
        record for _, _, _, record in sorted(
            (record.location, record.spec.idstr, idx, record)
            for idx, record in enumerate(self._lint))]

    # Remove any lint records that were suppressed at the line number where
    # they were recorded
    out = []
    events = list(self._suppression_events)
    active_suppressions = set()

    for record in records:
      if record.location:
        while events and record.location[0] >= events[0].lineno:
          event = events.pop(0)
          if event.mode == "add":
            for idstr in event.suppressions:
              active_suppressions.add(idstr)
          elif event.mode == "remove":
            for idstr in event.suppressions:
              active_suppressions.discard(idstr)
          else:
            raise ValueError("Illegal suppression event {}".format(event.mode))
      if record.spec.idstr in active_suppressions:
        continue
      out.append(record)

    return out

  def writeout(self, outfile):
    for record in self.get_lint():
      outfile.write("{:s}:{}\n".format(self.infile_path, record))

  def has_lint(self):
    return bool(self.get_lint())


class GlobalContext(object):
  category_names = {
      "C": "Convention",
      "E": "Error",
      "R": "Refactor",
      "W": "Warning",
  }

  def __init__(self, outfile):
    self.outfile = outfile
    self.lintdb = lintdb.get_database()
    self.file_ctxs = {}

  def get_file_ctx(self, infile_path, config):
    if infile_path not in self.file_ctxs:
      self.file_ctxs[infile_path] = FileContext(self, infile_path)
    ctx = self.file_ctxs[infile_path]
    ctx.config = config
    return ctx

  def get_category_counts(self):
    lint_counts = {}
    for _, file_ctx in sorted(self.file_ctxs.items()):
      for record in file_ctx.get_lint():
        category_char = record.spec.idstr[0]
        if category_char not in lint_counts:
          lint_counts[category_char] = 0
        lint_counts[category_char] += 1
    return lint_counts

  def write_summary(self, outfile):
    outfile.write("Summary\n=======\n")
    outfile.write("files scanned: {:d}\n".format(len(self.file_ctxs)))
    outfile.write("found lint:\n")

    lint_counts = self.get_category_counts()
    fieldwidth = max(len(name) for _, name in self.category_names.items())
    fmtstr = "  {:>" + str(fieldwidth) + "s}: {:d}\n"
    for (category_char, count) in sorted(lint_counts.items()):
      category_name = self.category_names[category_char]
      outfile.write(fmtstr.format(category_name, count))
    outfile.write("\n")
