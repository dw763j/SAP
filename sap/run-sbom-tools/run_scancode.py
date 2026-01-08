import os
import time

from util import read_directories, run_command_with_timeout, get_filename, write_to_file


def read_dir_run():
    root_path = '/mnt/sbom-measure'
    txt_path = f'{root_path}/metadata-files/docker-fullpath-java.txt'
    outputdir = f'{root_path}/sboms/results-java/'
    spdxlogFile = f'{root_path}/logs/generate-sboms-logs/scancode_cdx_java_log.txt'
    cmd_log_path = f'{root_path}/logs/generate-sboms-logs/scancode_run_java.log'

    write_to_file('Package', 'TIME', spdxlogFile)

    directories = read_directories(txt_path)

    for directory in directories:
        cmd = "scancode -clpieu --cyclonedx  %scdx#scancode#%s.json %s"
        fileName = get_filename(directory)
        already = f'{outputdir}cdx#scancode#{fileName}.json'
        if os.path.exists(already):
            print(f"File {already} already exists.")
            continue
        cmd = cmd % (outputdir, fileName, directory)

        start_time = time.time()
        run_command_with_timeout(cmd, 600, cmd_log_path)
        end_time = time.time()

        elapsed_time = int(end_time - start_time)
        write_to_file(fileName, str(elapsed_time), spdxlogFile)

        print(f"Finished scanning {fileName}")


read_dir_run()
