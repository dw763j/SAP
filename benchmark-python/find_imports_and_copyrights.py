
import sys
import os

standard_libs = ['csv', 'string', 're', 'difflib', 'textwrap', 'unicodedata', 'stringprep', 'readline', 'rlcompleter',
                 'struct', 'codecs', 'datetime', 'zoneinfo', 'calendar', 'collections', 'collections.abc', 'heapq', 'bisect', 'array',
                 'weakref', 'types', 'copy', 'pprint', 'reprlib', 'enum', 'graphlib', 'numbers', 'math', 'cmath', 'decimal', 'fractions',
                 'random', 'statistics', 'itertools', 'functools', 'operator', 'pathlib', 'os.path', 'fileinput', 'stat', 'filecmp',
                 'tempfile', 'glob', 'fnmatch', 'linecache', 'shutil', 'pickle', 'copyreg', 'shelve', 'marshal', 'dbm', 'sqlite3',
                 'zlib', 'gzip', 'bz2', 'lzma', 'zipfile', 'tarfile', 'configparser', 'tomllib', 'netrc', 'plistlib', 'hashlib', 'hmac',
                 'secrets', 'os', 'io', 'time', 'argparse', 'getopt', 'logging', 'logging.config', 'logging.handlers', 'getpass',
                 'curses', 'curses.textpad', 'curses.ascii', 'curses.panel', 'platform', 'errno', 'ctypes', 'threading', 'multiprocessing',
                 'multiprocessing.shared_memory', 'The', 'concurrent.futures', 'subprocess', 'sched', 'queue', 'contextvars', '_thread',
                 'asyncio', 'socket', 'ssl', 'select', 'selectors', 'signal', 'mmap', 'email', 'json', 'mailbox', 'mimetypes', 'base64',
                 'binascii', 'quopri', 'html', 'html.parser', 'html.entities', 'xml.etree.ElementTree', 'xml.dom', 'xml.dom.minidom',
                 'xml.dom.pulldom', 'xml.sax', 'xml.sax.handler', 'xml.sax.saxutils', 'xml.sax.xmlreader', 'xml.parsers.expat', 'webbrowser',
                 'wsgiref', 'urllib', 'urllib.request', 'urllib.response', 'urllib.parse', 'urllib.error.request', 'urllib.robotparser.txt',
                 'http', 'http.client', 'ftplib', 'poplib', 'imaplib', 'smtplib', 'uuid', 'socketserver', 'http.server', 'http.cookies',
                 'http.cookiejar', 'xmlrpc', 'xmlrpc.client-RPC', 'xmlrpc.server', 'ipaddress', 'wave', 'colorsys', 'gettext', 'locale',
                 'turtle', 'cmd', 'shlex', 'tkinter', 'tkinter.colorchooser', 'tkinter.font', 'tkinter.messagebox', 'tkinter.scrolledtext',
                 'tkinter.dnd', 'tkinter.ttk', 'tkinter.tix', 'IDLE', 'typing', 'pydoc', 'doctest', 'unittest', 'unittest.mock',
                 'unittest.mock', '2to3', 'test', 'test.support', 'test.support.socket_helper', 'test.support.script_helper',
                 'test.support.bytecode_helper', 'test.support.threading_helper', 'test.support.os_helper', 'test.support.import_helper',
                 'test.support.warnings_helper', 'bdb', 'faulthandler', 'pdb', 'timeit', 'trace', 'tracemalloc', 'ensurepip', 'venv',
                 'zipapp', 'sys', 'sys.monitoring', 'sysconfig', 'builtins', '__main__', 'warnings', 'dataclasses', 'contextlib',
                 'abc', 'atexit', 'traceback', '__future__', 'gc', 'inspect', 'site', 'code', 'codeop', 'zipimport', 'pkgutil',
                 'modulefinder', 'runpy', 'importlib', 'importlib.resources', 'importlib.resources.abc', 'importlib.metadata', 'sys.path',
                 'ast', 'symtable', 'token', 'keyword', 'tokenize', 'tabnanny', 'pyclbr', 'py_compile', 'compileall', 'dis', 'pickletools',
                 'msvcrt', 'winreg', 'winsound', 'posix', 'pwd', 'grp', 'termios', 'tty', 'pty', 'fcntl', 'resource', 'aifc', 'audioop',
                 'cgi', 'cgitb', 'chunk', 'crypt', 'imghdr', 'mailcap', 'msilib', 'nis', 'nntplib', 'optparse', 'ossaudiodev', 'pipes',
                 'sndhdr', 'spwd', 'sunau', 'telnetlib', 'uu', 'xdrlib']


def find_imports(folder_path):
    print(f"========================================== imports and copyrights in {folder_path} =============================================")
    printed_import = []
    printed_copyright = []
    for root, dirs, files in os.walk(folder_path):
        # Exclude the .git directory
        if ".git" in dirs:
            dirs.remove(".git")
        for file in files:
            if file.endswith(".py") or "LICENSE" in file or file.endswith(".md"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    for line_num, line in enumerate(f, start=1):
                        if line.startswith("import") or line.startswith("from") and "import" in line:
                            if line.startswith("from ."):
                                continue
                            if line.split()[1] not in standard_libs:
                                li = line.strip()
                                if li not in printed_import:
                                    printed_import.append(li)
                                    print(f"[Import]: {li}")
                        if "copyright" in line or "Copyright" in line or "(c)" in line:
                            li = line.strip()
                            if li not in printed_copyright:
                                printed_copyright.append(li)
                                print("\033[94m" + f"[Copyright]: {li}" + "\033[0m")
    print(f"========================================= imports and copyrights in {folder_path} ends ===========================================")


# Example usage
# folder_path = "/mnt/github-python/uiautomator2"
# Get folder_path from command line argument
folder_path = sys.argv[1]
find_imports(folder_path)
# Read lines from interactive input
