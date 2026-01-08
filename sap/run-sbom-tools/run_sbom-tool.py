import time

from util import delete_directory, read_directories, run_command_with_timeout, get_filename, write_to_file


def run():
    root_path = '/mnt/sbom-measure'
    outputdir = f'{root_path}/sboms/results-java/'
    tmpDir = f'{root_path}/tmp-dir/tmp-sbom-tool-java/'
    txt_path = f'{root_path}/metadata-files/docker-fullpath-java.txt'

    spdxlogFile = f'{root_path}/logs/generate-sboms-logs/sbom-tool-spdx-log-java.txt'
    cmd_log_path = f'{root_path}/logs/generate-sboms-logs/sbom-tool-run-java.log'

    directories = read_directories(txt_path)
    for directory in directories:

        fileName = get_filename(directory)
        # spdxout = outputdir + f"spdx#sbom-tool#{fileName}.json"

        cmd1 = "sbom-tool generate -b %s -bc %s -ps %s -pm true -li true -m %s -pn %s -pv %s-15.4.6"
        cmd1 = cmd1 % (directory, directory, fileName, tmpDir, fileName, fileName)

        start_time = time.time()
        run_command_with_timeout(cmd1, 600, cmd_log_path)
        end_time = time.time()

        elapsed_time = int(end_time - start_time)
        write_to_file(fileName, str(elapsed_time), spdxlogFile)

        # run_command_with_timeout(cmd, 5, cmd_log_path)
        cmd2 = f'mv {tmpDir}_manifest/spdx_2.2/manifest.spdx.json {outputdir}"spdx#sbom-tool#"{fileName}".json"'
        run_command_with_timeout(cmd2, 5, cmd_log_path)
        # delete the `_manifest` dir
        delete_directory(f"{tmpDir}_manifest")


run()
