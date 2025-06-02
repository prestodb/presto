from __future__ import unicode_literals
import contextlib
import difflib
import io
import os
import shutil
import subprocess
import sys
import unittest
import tempfile


class TestInvocations(unittest.TestCase):

  def __init__(self, *args, **kwargs):
    super(TestInvocations, self).__init__(*args, **kwargs)
    self.tempdir = None
    self.tempconfig = None

  def setUp(self):
    self.tempdir = tempfile.mkdtemp(prefix='cmakeformattest_')
    thisdir = os.path.realpath(os.path.dirname(__file__))
    parentdir = os.path.dirname(thisdir)

    self.env = {
        'PYTHONPATH': os.path.dirname(parentdir)
    }

    configpath = os.path.join(thisdir, 'testdata', 'cmake-format.py')
    self.tempconfig = os.path.join(self.tempdir, '.cmake-format.py')
    shutil.copyfile(configpath, self.tempconfig)

  @contextlib.contextmanager
  def subTest(self, msg=None, **params):
    # pylint: disable=no-member
    if sys.version_info < (3, 4, 0):
      yield None
    else:
      yield super(TestInvocations, self).subTest(msg=msg, **params)

  def tearDown(self):
    shutil.rmtree(self.tempdir)

  def test_pipeout_invocation(self):
    """
    Test invocation with an infile path and output to stdout.
    """

    thisdir = os.path.realpath(os.path.dirname(__file__))
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    with io.open(os.path.join(self.tempdir, 'test_out.cmake'), 'wb') as outfile:
      subprocess.check_call([sys.executable, '-Bm', 'cmakelang.format',
                             infile_path], stdout=outfile, cwd=self.tempdir,
                            env=self.env)

    with io.open(os.path.join(self.tempdir, 'test_out.cmake'), 'r',
                 encoding='utf8') as infile:
      actual_text = infile.read()
    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_fileout_invocation(self):
    """
    Test invocation with an infile path and outfile path
    """

    thisdir = os.path.realpath(os.path.dirname(__file__))
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    subprocess.check_call([sys.executable, '-Bm', 'cmakelang.format',
                           '-o', os.path.join(self.tempdir, 'test_out.cmake'),
                           infile_path], cwd=self.tempdir, env=self.env)

    with io.open(os.path.join(self.tempdir, 'test_out.cmake'), 'r',
                 encoding='utf8') as infile:
      actual_text = infile.read()
    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_inplace_invocation(self):
    """
    Test invocation for inplace format of a file
    """

    thisdir = os.path.realpath(os.path.dirname(__file__))
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    ofd, tmpfile_path = tempfile.mkstemp(suffix='.txt', prefix='CMakeLists',
                                         dir=self.tempdir)
    os.close(ofd)
    shutil.copyfile(infile_path, tmpfile_path)
    os.chmod(tmpfile_path, 0o755)
    subprocess.check_call([sys.executable, '-Bm', 'cmakelang.format',
                           '-i', tmpfile_path], cwd=self.tempdir, env=self.env)

    with io.open(os.path.join(tmpfile_path), 'r', encoding='utf8') as infile:
      actual_text = infile.read()
    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

    # Verify permissions are preserved
    self.assertEqual(oct(os.stat(tmpfile_path).st_mode)[-3:], "755")

  def test_check_invocation(self):
    """
    Test invocation for --check of a file
    """

    thisdir = os.path.realpath(os.path.dirname(__file__))
    unformatted_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    formatted_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    with open("/dev/null", "wb") as devnull:
      statuscode = subprocess.call([
          sys.executable, '-Bm', 'cmakelang.format',
          '--check', unformatted_path], env=self.env, stderr=devnull)
    self.assertEqual(1, statuscode)

    statuscode = subprocess.call([
        sys.executable, '-Bm', 'cmakelang.format',
        '--check', formatted_path], env=self.env)
    self.assertEqual(0, statuscode)

  def test_stream_invocation(self):
    """
    Test invocation with stdin as the infile and stdout as the outifle
    """

    thisdir = os.path.realpath(os.path.dirname(__file__))
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    stdinpipe = os.pipe()
    stdoutpipe = os.pipe()

    def preexec():
      os.close(stdinpipe[1])
      os.close(stdoutpipe[0])

    # pylint: disable=W1509
    proc = subprocess.Popen([sys.executable, '-Bm', 'cmakelang.format', '-'],
                            stdin=stdinpipe[0], stdout=stdoutpipe[1],
                            cwd=self.tempdir, env=self.env, preexec_fn=preexec)
    os.close(stdinpipe[0])
    os.close(stdoutpipe[1])

    with io.open(infile_path, 'r', encoding='utf-8') as infile:
      with io.open(stdinpipe[1], 'w', encoding='utf-8') as outfile:
        for line in infile:
          outfile.write(line)

    with io.open(stdoutpipe[0], 'r', encoding='utf-8') as infile:
      actual_text = infile.read()

    proc.wait()

    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_encoding_invocation(self):
    """
    Try to reformat latin1-encoded file, once with default
    encoding (-> prompt utf8-decoding error) and once with
    specifically latin1 encoding (-> should succeed)
    """

    thisdir = os.path.realpath(os.path.dirname(__file__))
    infile_path = os.path.join(thisdir, 'testdata', 'test_latin1_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_latin1_out.cmake')

    # this invocation should fail
    invocation_result = subprocess.call(
        [sys.executable, '-Bm', 'cmakelang.format',
         '--outfile-path', os.path.join(self.tempdir, 'test_latin1_out.cmake'),
         infile_path],
        cwd=self.tempdir, env=self.env,
        stderr=subprocess.PIPE)
    self.assertNotEqual(
        0, invocation_result,
        msg="Expected cmake-format invocation to fail but did not")

    # this invocation should succeed
    subprocess.check_call(
        [sys.executable, '-Bm', 'cmakelang.format',
         '--input-encoding=latin1',
         '--output-encoding=latin1',
         '--outfile-path', os.path.join(self.tempdir, 'test_latin1_out.cmake'),
         infile_path],
        cwd=self.tempdir, env=self.env)

    with io.open(os.path.join(self.tempdir, 'test_latin1_out.cmake'), 'r',
                 encoding='latin1') as infile:
      actual_text = infile.read()

    with io.open(expectfile_path, 'r', encoding='latin1') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_no_config_invocation(self):
    """
    Test invocation with no config file specified
    """
    os.unlink(self.tempconfig)
    thisdir = os.path.realpath(os.path.dirname(__file__))
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    subprocess.check_call([sys.executable, '-Bm', 'cmakelang.format',
                           '-o', os.path.join(self.tempdir, 'test_out.cmake'),
                           infile_path], cwd=self.tempdir, env=self.env)

    with io.open(os.path.join(self.tempdir, 'test_out.cmake'), 'r',
                 encoding='utf8') as infile:
      actual_text = infile.read()
    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_multiple_config_invocation(self):
    """
    Repeat the default config test using a config file that is split. The
    custom command definitions are in the second file.
    """
    thisdir = os.path.realpath(os.path.dirname(__file__))
    configdir = os.path.join(thisdir, 'testdata')
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    subprocess.check_call([
        sys.executable, '-Bm', 'cmakelang.format', '-c',
        os.path.join(configdir, 'cmake-format-split-1.py'),
        os.path.join(configdir, 'cmake-format-split-2.py'),
        '-o', os.path.join(self.tempdir, 'test_out.cmake'),
        infile_path], cwd=self.tempdir, env=self.env)

    with io.open(os.path.join(self.tempdir, 'test_out.cmake'), 'r',
                 encoding='utf8') as infile:
      actual_text = infile.read()
    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_multiple_config_invocation_dupflag(self):
    """
    Repeat the default config test using a config file that is split. The
    custom command definitions are in the second file.
    """
    thisdir = os.path.realpath(os.path.dirname(__file__))
    configdir = os.path.join(thisdir, 'testdata')
    infile_path = os.path.join(thisdir, 'testdata', 'test_in.cmake')
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')

    subprocess.check_call([
        sys.executable, '-Bm', 'cmakelang.format',
        '-c', os.path.join(configdir, 'cmake-format-split-1.py'),
        '-c', os.path.join(configdir, 'cmake-format-split-2.py'),
        '-o', os.path.join(self.tempdir, 'test_out.cmake'),
        infile_path], cwd=self.tempdir, env=self.env)

    with io.open(os.path.join(self.tempdir, 'test_out.cmake'), 'r',
                 encoding='utf8') as infile:
      actual_text = infile.read()
    with io.open(expectfile_path, 'r', encoding='utf8') as infile:
      expected_text = infile.read()

    delta_lines = list(difflib.unified_diff(expected_text.split('\n'),
                                            actual_text.split('\n')))
    if delta_lines:
      raise AssertionError('\n'.join(delta_lines[2:]))

  def test_auto_lineendings(self):
    """
    Verify that windows line-endings are detected and preserved on input.
    """
    thisdir = os.path.realpath(os.path.dirname(__file__))
    for suffix in ["win", "unix"]:
      with self.subTest(lineending=suffix):
        filename = "test_lineend_{}.cmake".format(suffix)
        infile_path = os.path.join(thisdir, 'testdata', filename)
        outfile_path = os.path.join(self.tempdir, filename)
        subprocess.check_call([sys.executable, '-Bm', 'cmakelang.format',
                               '--line-ending=auto',
                               '-o', outfile_path, infile_path],
                              cwd=self.tempdir, env=self.env)
        with open(infile_path, "rb") as infile:
          infile_bytes = infile.read()
        with open(outfile_path, "rb") as infile:
          outfile_bytes = infile.read()
        self.assertEqual(infile_bytes, outfile_bytes)

  def test_notouch(self):
    """
    Verify that, if formatting is unchanged, an --in-place file is not modified
    """
    thisdir = os.path.realpath(os.path.dirname(__file__))
    expectfile_path = os.path.join(thisdir, 'testdata', 'test_out.cmake')
    outfile_path = os.path.join(self.tempdir, 'test_out.cmake')
    shutil.copy2(expectfile_path, outfile_path)
    mtime_before = os.path.getmtime(outfile_path)
    subprocess.check_call(
        [sys.executable, '-Bm', 'cmakelang.format', '-i', outfile_path],
        cwd=self.tempdir, env=self.env)
    mtime_after = os.path.getmtime(outfile_path)
    self.assertEqual(mtime_before, mtime_after)

  def test_require_valid(self):
    """
    Verify that the --require-valid-layout flag works as intended
    """
    thisdir = os.path.realpath(os.path.dirname(__file__))
    testfilepath = os.path.join(thisdir, 'testdata', 'test_invalid.cmake')

    with tempfile.NamedTemporaryFile(
        suffix=".txt", prefix="CMakeLists", dir=self.tempdir) as outfile:
      statuscode = subprocess.call(
          [sys.executable, '-Bm', 'cmakelang.format', testfilepath],
          stdout=outfile, stderr=outfile, env=self.env)
    self.assertEqual(0, statuscode)

    with tempfile.NamedTemporaryFile(
        suffix=".txt", prefix="CMakeLists", dir=self.tempdir) as outfile:
      statuscode = subprocess.call(
          [sys.executable, '-Bm', 'cmakelang.format', testfilepath,
           "--require-valid-layout"],
          stdout=outfile, stderr=outfile, env=self.env)
    self.assertEqual(1, statuscode)


if __name__ == '__main__':
  unittest.main()
