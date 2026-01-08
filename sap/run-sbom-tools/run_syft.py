import time

from util import read_directories, run_command_with_timeout, get_filename, write_to_file


def run():
    root_path = '/mnt/sbom-measure'
    outputdir = f'{root_path}/results-python-syft-1.14.0/'
    txt_path = f'{root_path}/metadata-files/docker-fullpath-python.txt'

    spdxlogFile = f'{root_path}/logs/generate-sboms-logs/syft_spdx_python_log.txt'
    cdxlogFile = f'{root_path}/logs/generate-sboms-logs/syft_cdx_python_log.txt'
    cmd_log_path = f'{root_path}/logs/generate-sboms-logs/run_syft-python.log'

    directories = read_directories(txt_path)

    for directory in directories:

        fileName = get_filename(directory)
        cdxout = outputdir + f"cdx#syft#{fileName}.json"
        spdxout = outputdir + f"spdx#syft#{fileName}.json"

        cmd = f"syft {directory} -o spdx-json={spdxout}"
        start_time = time.time()
        run_command_with_timeout(cmd, 600, cmd_log_path)

        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        write_to_file(fileName, str(elapsed_time), spdxlogFile)

        cmd = f"syft {directory} -o cyclonedx-json={cdxout}"
        start_time = time.time()
        run_command_with_timeout(cmd, 600, cmd_log_path)
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        write_to_file(fileName, str(elapsed_time), cdxlogFile)


run()
