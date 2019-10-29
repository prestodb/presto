#!/usr/bin/env python

import errno
import os
import platform
import sys
import traceback

from fcntl import flock, LOCK_EX, LOCK_NB
from optparse import OptionParser
from os import O_RDWR, O_CREAT, O_WRONLY, O_APPEND
from os.path import basename, dirname, exists, realpath
from os.path import join as pathjoin
from signal import SIGTERM, SIGKILL
from stat import S_ISLNK
from time import sleep

COMMANDS = ['run', 'start', 'stop', 'restart', 'kill', 'status']

LSB_NOT_RUNNING = 3
LSB_STATUS_UNKNOWN = 4


def find_install_path(f):
    """Find canonical parent of bin/launcher.py"""
    if basename(f) != 'launcher.py':
        raise Exception("Expected file '%s' to be 'launcher.py' not '%s'" % (f, basename(f)))
    p = realpath(dirname(f))
    if basename(p) != 'bin':
        raise Exception("Expected file '%s' directory to be 'bin' not '%s" % (f, basename(p)))
    return dirname(p)


def makedirs(p):
    """Create directory and all intermediate ones"""
    try:
        os.makedirs(p)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def load_properties(f):
    """Load key/value pairs from a file"""
    properties = {}
    for line in load_lines(f):
        k, v = line.split('=', 1)
        properties[k.strip()] = v.strip()
    return properties


def load_lines(f):
    """Load lines from a file, ignoring blank or comment lines"""
    lines = []
    for line in open(f, 'r').readlines():
        line = line.strip()
        if len(line) > 0 and not line.startswith('#'):
            lines.append(line)
    return lines


def try_lock(f):
    """Try to open an exclusive lock (inheritable) on a file"""
    try:
        flock(f, LOCK_EX | LOCK_NB)
        return True
    except (IOError, OSError):  # IOError in Python 2, OSError in Python 3.
        return False


def open_read_write(f, mode):
    """Open file in read/write mode (without truncating it)"""
    return os.fdopen(os.open(f, O_RDWR | O_CREAT, mode), 'r+')


class Process:
    def __init__(self, path):
        makedirs(dirname(path))
        self.path = path
        self.pid_file = open_read_write(path, 0o600)
        self.refresh()

    def refresh(self):
        self.locked = try_lock(self.pid_file)

    def clear_pid(self):
        assert self.locked, 'pid file not locked by us'
        self.pid_file.seek(0)
        self.pid_file.truncate()

    def write_pid(self, pid):
        self.clear_pid()
        self.pid_file.write(str(pid) + '\n')
        self.pid_file.flush()

    def alive(self):
        self.refresh()
        if self.locked:
            return False

        pid = self.read_pid()
        try:
            os.kill(pid, 0)
            return True
        except OSError as e:
            raise Exception('Signaling pid %s failed: %s' % (pid, e))

    def read_pid(self):
        assert not self.locked, 'pid file is locked by us'
        self.pid_file.seek(0)
        line = self.pid_file.readline().strip()
        if len(line) == 0:
            raise Exception("Pid file '%s' is empty" % self.path)

        try:
            pid = int(line)
        except ValueError:
            raise Exception("Pid file '%s' contains garbage: %s" % (self.path, line))
        if pid <= 0:
            raise Exception("Pid file '%s' contains an invalid pid: %s" % (self.path, pid))
        return pid


def redirect_stdin_to_devnull():
    """Redirect stdin to /dev/null"""
    fd = os.open(os.devnull, O_RDWR)
    os.dup2(fd, sys.stdin.fileno())
    os.close(fd)


def open_append(f):
    """Open a raw file descriptor in append mode"""
    # noinspection PyTypeChecker
    return os.open(f, O_WRONLY | O_APPEND | O_CREAT, 0o644)


def redirect_output(fd):
    """Redirect stdout and stderr to a file descriptor"""
    os.dup2(fd, sys.stdout.fileno())
    os.dup2(fd, sys.stderr.fileno())


def symlink_exists(p):
    """Check if symlink exists and raise if another type of file exists"""
    try:
        st = os.lstat(p)
        if not S_ISLNK(st.st_mode):
            raise Exception('Path exists and is not a symlink: %s' % p)
        return True
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    return False


def create_symlink(source, target):
    """Create a symlink, removing the target first if it is a symlink"""
    if symlink_exists(target):
        os.remove(target)
    if exists(source):
        os.symlink(source, target)


def create_app_symlinks(options):
    """
    Symlink the 'etc' and 'plugin' directory into the data directory.

    This is needed to support programs that reference 'etc/xyz' from within
    their config files: log.levels-file=etc/log.properties
    """
    if options.etc_dir != pathjoin(options.data_dir, 'etc'):
        create_symlink(
            options.etc_dir,
            pathjoin(options.data_dir, 'etc'))

    if options.install_path != options.data_dir:
        create_symlink(
            pathjoin(options.install_path, 'plugin'),
            pathjoin(options.data_dir, 'plugin'))


def build_java_execution(options, daemon):
    if not exists(options.config_path):
        raise Exception('Config file is missing: %s' % options.config_path)
    if not exists(options.jvm_config):
        raise Exception('JVM config file is missing: %s' % options.jvm_config)
    if not exists(options.launcher_config):
        raise Exception('Launcher config file is missing: %s' % options.launcher_config)
    if options.log_levels_set and not exists(options.log_levels):
        raise Exception('Log levels file is missing: %s' % options.log_levels)

    properties = options.properties.copy()

    if exists(options.log_levels):
        properties['log.levels-file'] = options.log_levels

    if daemon or options.server_log:
        server_log = realpath(options.server_log or pathjoin(options.data_dir, 'var/log/server.log'))
        properties['log.output-file'] = server_log
        properties['log.enable-central-logging'] = options.enable_central_logging
        if not options.enable_console:
            properties['log.enable-console'] = 'false'

    env = os.environ.copy()
    jvm_properties = load_lines(options.jvm_config)
    if options.debug_port > 0:
        suspend_char = 'y' if options.debug_suspend else 'n'
        jvm_properties.append('-agentlib:jdwp=transport=dt_socket,server=y,suspend=' + suspend_char + ',address=' + str(options.debug_port))
    if 'JVM_EXTRA_OPTS' in env:
        for f in env['JVM_EXTRA_OPTS'].strip().split(' '):
            jvm_properties.append(f)
    launcher_properties = load_properties(options.launcher_config)

    try:
        main_class = launcher_properties['main-class']
    except KeyError:
        raise Exception("Launcher config is missing 'main-class' property")

    properties['config'] = options.config_path

    system_properties = ['-D%s=%s' % i for i in properties.items()]
    classpath = pathjoin(options.install_path, 'lib', '*')

    command = ['java', '-cp', classpath]
    command += jvm_properties + system_properties
    command += [main_class]

    if options.verbose:
        print(command)
        print("")

    # set process name: https://github.com/electrum/procname
    process_name = launcher_properties.get('process-name', '')
    if len(process_name) > 0:
        system = platform.system() + '-' + platform.machine()
        shim = pathjoin(options.install_path, 'bin', 'procname', system, 'libprocname.so')
        if exists(shim):
            env['LD_PRELOAD'] = (env.get('LD_PRELOAD', '') + ':' + shim).strip()
            env['PROCNAME'] = process_name

    return command, env


def run(process, options):
    if process.alive():
        print('Already running as %s' % process.read_pid())
        return

    create_app_symlinks(options)
    args, env = build_java_execution(options, False)

    makedirs(options.data_dir)
    os.chdir(options.data_dir)

    process.write_pid(os.getpid())

    redirect_stdin_to_devnull()

    os.execvpe(args[0], args, env)


def start(process, options):
    if process.alive():
        print('Already running as %s' % process.read_pid())
        return

    create_app_symlinks(options)
    args, env = build_java_execution(options, True)

    makedirs(dirname(options.launcher_log))
    log = open_append(options.launcher_log)

    makedirs(options.data_dir)
    os.chdir(options.data_dir)

    pid = os.fork()
    if pid > 0:
        process.write_pid(pid)
        print('Started as %s' % pid)
        return

    if hasattr(os, "set_inheritable"):
        # See https://docs.python.org/3/library/os.html#inheritance-of-file-descriptors
        # Since Python 3.4
        os.set_inheritable(process.pid_file.fileno(), True)

    os.setsid()

    redirect_stdin_to_devnull()
    redirect_output(log)
    os.close(log)

    os.execvpe(args[0], args, env)


def terminate(process, signal, message):
    if not process.alive():
        print('Not running')
        return

    pid = process.read_pid()

    while True:
        try:
            os.kill(pid, signal)
        except OSError as e:
            if e.errno != errno.ESRCH:
                raise Exception('Signaling pid %s failed: %s' % (pid, e))

        if not process.alive():
            process.clear_pid()
            break

        sleep(0.1)

    print('%s %s' % (message, pid))


def stop(process):
    terminate(process, SIGTERM, 'Stopped')


def kill(process):
    terminate(process, SIGKILL, 'Killed')


def status(process):
    if not process.alive():
        print('Not running')
        sys.exit(LSB_NOT_RUNNING)
    print('Running as %s' % process.read_pid())


def handle_command(command, options):
    process = Process(options.pid_file)
    if command == 'run':
        run(process, options)
    elif command == 'start':
        start(process, options)
    elif command == 'stop':
        stop(process)
    elif command == 'restart':
        stop(process)
        start(process, options)
    elif command == 'kill':
        kill(process)
    elif command == 'status':
        status(process)
    else:
        raise AssertionError('Unhandled command: ' + command)


def create_parser():
    commands = 'Commands: ' + ', '.join(COMMANDS)
    parser = OptionParser(prog='launcher', usage='usage: %prog [options] command', description=commands)
    parser.add_option('-v', '--verbose', action='store_true', default=False, help='Run verbosely')
    parser.add_option('--etc-dir', metavar='DIR', help='Defaults to INSTALL_PATH/etc')
    parser.add_option('--launcher-config', metavar='FILE', help='Defaults to INSTALL_PATH/bin/launcher.properties')
    parser.add_option('--node-config', metavar='FILE', help='Defaults to ETC_DIR/node.properties')
    parser.add_option('--jvm-config', metavar='FILE', help='Defaults to ETC_DIR/jvm.config')
    parser.add_option('--config', metavar='FILE', help='Defaults to ETC_DIR/config.properties')
    parser.add_option('--log-levels-file', metavar='FILE', help='Defaults to ETC_DIR/log.properties')
    parser.add_option('--data-dir', metavar='DIR', help='Defaults to INSTALL_PATH')
    parser.add_option('--pid-file', metavar='FILE', help='Defaults to DATA_DIR/var/run/launcher.pid')
    parser.add_option('--launcher-log-file', metavar='FILE', help='Defaults to DATA_DIR/var/log/launcher.log')
    parser.add_option('--server-log-file', metavar='FILE', help='Defaults to stderr')
    parser.add_option('--enable-central-logging', action="store_true", default=False)
    parser.add_option('--debug_port', help='defaults to None', default=0, dest='debug_port', type=int)
    parser.add_option('--debug_suspend', action="store_true", dest='debug_suspend')
    parser.add_option('--enable-console', action="store_true", dest='enable_console')
    parser.add_option('-D', action='append', metavar='NAME=VALUE', dest='properties', help='Set a Java system property')
    return parser


def parse_properties(parser, args):
    properties = {}
    for arg in args:
        if '=' not in arg:
            parser.error('property is malformed: %s' % arg)
        key, value = [i.strip() for i in arg.split('=', 1)]
        if key == 'config':
            parser.error('cannot specify config using -D option (use --config)')
        if key == 'log.output-file':
            parser.error('cannot specify server log using -D option (use --server-log-file)')
        if key == 'log.levels-file':
            parser.error('cannot specify log levels using -D option (use --log-levels-file)')
        properties[key] = value
    return properties


def print_options(options):
    if options.verbose:
        for i in sorted(vars(options)):
            print("%-15s = %s" % (i, getattr(options, i)))
        print("")


class Options:
    pass


def main():
    parser = create_parser()

    (options, args) = parser.parse_args()

    if len(args) != 1:
        if len(args) == 0:
            parser.error('command name not specified')
        else:
            parser.error('too many arguments')
    command = args[0]

    if command not in COMMANDS:
        parser.error('unsupported command: %s' % command)

    try:
        install_path = find_install_path(sys.argv[0])
    except Exception as e:
        print('ERROR: %s' % e)
        sys.exit(LSB_STATUS_UNKNOWN)

    o = Options()
    o.verbose = options.verbose
    o.install_path = install_path
    o.launcher_config = realpath(options.launcher_config or pathjoin(o.install_path, 'bin/launcher.properties'))
    o.etc_dir = realpath(options.etc_dir or pathjoin(o.install_path, 'etc'))
    o.node_config = realpath(options.node_config or pathjoin(o.etc_dir, 'node.properties'))
    o.jvm_config = realpath(options.jvm_config or pathjoin(o.etc_dir, 'jvm.config'))
    o.config_path = realpath(options.config or pathjoin(o.etc_dir, 'config.properties'))
    o.log_levels = realpath(options.log_levels_file or pathjoin(o.etc_dir, 'log.properties'))
    o.log_levels_set = bool(options.log_levels_file)
    o.enable_central_logging = options.enable_central_logging

    if options.node_config and not exists(o.node_config):
        parser.error('Node config file is missing: %s' % o.node_config)

    node_properties = {}
    if exists(o.node_config):
        node_properties = load_properties(o.node_config)

    data_dir = node_properties.get('node.data-dir')
    o.data_dir = realpath(options.data_dir or data_dir or o.install_path)

    o.pid_file = realpath(options.pid_file or pathjoin(o.data_dir, 'var/run/launcher.pid'))
    o.launcher_log = realpath(options.launcher_log_file or pathjoin(o.data_dir, 'var/log/launcher.log'))
    o.server_log = options.server_log_file
    o.debug_port = options.debug_port
    o.debug_suspend = options.debug_suspend
    o.enable_console = options.enable_console

    o.properties = parse_properties(parser, options.properties or {})
    for k, v in node_properties.items():
        if k not in o.properties:
            o.properties[k] = v

    if o.verbose:
        print_options(o)

    try:
        handle_command(command, o)
    except SystemExit:
        raise
    except Exception as e:
        if o.verbose:
            traceback.print_exc()
        else:
            print('ERROR: %s' % e)
        sys.exit(LSB_STATUS_UNKNOWN)


if __name__ == '__main__':
    main()
