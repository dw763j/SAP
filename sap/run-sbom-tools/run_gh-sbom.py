import os
import time

from util import read_directories, run_command_with_timeout, get_filename, write_to_file


def read_dir_run():
    root_path = '/mnt/sbom-measure'
    outputdir = f'{root_path}/sboms/results-c-cpp/'
    cmd_log_path = f'{root_path}/logs/generate-sboms-logs/gh-sbom-run-c-cpp.log'
    spdxlogFile = f'{root_path}/logs/generate-sboms-logs/gh-sbom_spdx_c-cpp_Log.txt'
    cdxlogFile = f'{root_path}/logs/generate-sboms-logs/gh-sbom_cdx_c-cpp_Log.txt'

    write_to_file('TOOL', 'Package', 'TIME', spdxlogFile)
    write_to_file('TOOL', 'Package', 'TIME', cdxlogFile)

    directories = read_directories(f'{root_path}/metadata-files/docker-fullpath-c-cpp.txt')

    for directory in directories:

        # git add the dir or we will get error
        cmd1 = f"git config --global --add safe.directory {directory}"
        run_command_with_timeout(cmd1, 5, cmd_log_path)
        fileName = get_filename(directory)
        cdxout = outputdir + f"cdx#gh-sbom#{fileName}.json"
        spdxout = outputdir + f"spdx#gh-sbom#{fileName}.json"

        cmd2 = f"cd {directory} && echo $(gh sbom | jq) > {spdxout}"
        if os.path.exists(spdxout):
            # if spedxout file exist but nothing in the file, we need to run the command again
            if os.path.getsize(spdxout) == 1:
                print(f"File {spdxout} exists but nothing in the file.")
                pass
            else:
                print(f"File {spdxout} already exists.")
                cmd2 = f"echo {spdxout} exists."
        start_time = time.time()
        run_command_with_timeout(cmd2, 600, cmd_log_path)
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        write_to_file("gh-sbom", fileName, str(elapsed_time), spdxlogFile)

        cmd3 = f"cd {directory} && echo $(gh sbom -c -l | jq) > {cdxout}"
        if os.path.exists(cdxout):
            if os.path.getsize(cdxout) == 1:
                print(f"File {cdxout} exists but nothing in the file.")
                pass
            else:
                print(f"File {cdxout} already exists.")
                cmd3 = f"echo {cdxout} exists."
        start_time = time.time()
        run_command_with_timeout(cmd3, 600, cmd_log_path)
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        write_to_file("gh-sbom", fileName, str(elapsed_time), cdxlogFile)


read_dir_run()
