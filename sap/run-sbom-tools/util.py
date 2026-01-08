import os
import shutil
import subprocess


def read_directories(txt_path):
    with open(txt_path, 'r') as f:
        directories = []
        for line in f:
            if (line == '\n'):
                continue
            directory = line.strip('\n')
            directories.append(directory)
        return directories


def run_command_with_timeout(cmd, timeout_sec, cmd_log_path):
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, timeout=timeout_sec)
        stdout, stderr = result.stdout.decode('utf-8'), result.stderr.decode('utf-8')
    except subprocess.TimeoutExpired:
        stdout, stderr = None, f'Command \"{cmd}\" timed out after {timeout_sec} seconds'.encode('utf-8').decode('utf-8')
    except Exception as e:
        stdout, stderr = None, str(e)

    with open(cmd_log_path, 'a') as f:
        f.write(f"\n{cmd}")
        if stdout:
            f.write("\nSTDOUT:\n" + stdout)
        if stderr:
            f.write("\nSTDERR:\n" + stderr)


def delete_directory(path):
    try:
        shutil.rmtree(path)
        print(f"Directory {path} has been deleted successfully.")
    except OSError as e:
        print(f"Error: {e.strerror}  {path} ")


def get_filename(path):
    return os.path.basename(path)


def write_to_file(tool, package, time, filename):
    with open(filename, 'a') as f:
        f.write("{:<10} {:<20} {:<10}\n".format(tool, package, time))
