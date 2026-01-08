# $(pwd) is the package dir we need to analyse.
# $(outputDir) is the output dir we put output.

# docker run --rm -v $(outputDir):/tmp -v $(pwd):/app:rw -t ghcr.io/cyclonedx/cdxgen -r /app -o /tmp/cdx#cdxgen#$(packageName).json

# this file is run in the origin environment to use docker.
import os
import time
from util import read_directories, run_command_with_timeout, get_filename, write_to_file


def run():
    # all paths
    root_path = '/path/to/sbom-measure'
    outputdir = f'{root_path}/sboms/results-python-cdxgen-v10.10.4/'
    # tmpDir = f'{root_path}/tmp-dir/cdxgen-python-v10.10.4'
    txt_path = f'{root_path}/metadata-files/full-path-python.txt'

    cdxlogFile = f'{root_path}/logs/generate-sboms-logs/run_cdxgen_cdx_python_log-v10.10.4.txt'
    cmd_log_path = f'{root_path}/logs/generate-sboms-logs/run_cdxgen-python-v10.10.4.log'

    directories = read_directories(txt_path)

    '''
    pwd=/mnt/github-python/12-github-python/httptools
    outputDir=/root/script/
    packageName=cdxruntest
    docker run --rm -v $outputDir:/tmp -v $pwd:/app:rw -t ghcr.io/cyclonedx/cdxgen -r /app -o /tmp/cdx#cdxgen#$packageName.json
    '''
    for directory in directories:

        fileName = get_filename(directory)
        outfile_name = f"cdx#cdxgen#{fileName}.json"
        already = f"{outputdir}{outfile_name}"
        if os.path.exists(already):
            print(f"File {already} already exists.")
            continue
        cmd = f"docker run --rm -v {outputdir}:/tmp -v {directory}:/app:rw -t ghcr.io/cyclonedx/cdxgen:v10.10.4 -r /app -o /tmp/{outfile_name}"

        start_time = time.time()
        run_command_with_timeout(cmd, 600, cmd_log_path)
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        write_to_file("cdxgen", fileName, str(elapsed_time), cdxlogFile)


run()
