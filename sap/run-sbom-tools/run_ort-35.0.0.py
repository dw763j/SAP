import os

from util import read_directories, run_command_with_timeout, get_filename


def read_dir_run():
    root_path = '/mnt/sbom-measure'
    txt_path = f'{root_path}/metadata-files/docker-fullpath-c-cpp.txt'

    outputdir = f'{root_path}/sboms/ort-35.0.0-results-c-cpp/sbomfiles/'

    cmd_log_path = f'{root_path}/logs/generate-sboms-logs/ort-35.0.0-run-c-cpp.log'

    ort_tool_path = '/all-sbom-tools/ort-35.0.0/ort/cli/build/install/ort/bin/ort'

    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    # ort -P ort.analyzer.allowDynamicVersions=true analyze -i `input dir` -o `output dir`
    # /mnt/additional_exp/ort-results
    analyze_tmp_dir = f'{root_path}/tmp-dir/ort-35.0.0-results-c-cpp/analyzer-output/'
    # ort scan -i `path of analyze json` -o `out dir`
    scan_tmp_dir = f'{root_path}/tmp-dir/ort-35.0.0-results-c-cpp/scanner-output/'
    # ort report -i `scan result` -o `output dir` -f SpdxDocument,CycloneDx
    report_tmp_dir = f'{root_path}/tmp-dir/ort-35.0.0-results-c-cpp/reporter-output/'
    for dir in [analyze_tmp_dir, scan_tmp_dir, report_tmp_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)

    directories = read_directories(txt_path)

    for directory in directories:
        fileName = get_filename(directory)

        cmd1 = f'{ort_tool_path} -P ort.analyzer.allowDynamicVersions=true analyze -i {directory} -o {analyze_tmp_dir}{fileName}'
        run_command_with_timeout(cmd1, 600, cmd_log_path)
        print(f'{fileName} ort analyzer finished.')

        analyzed_result = f'{analyze_tmp_dir}{fileName}/analyzer-result.yml'
        cmd2 = f'{ort_tool_path} scan -i {analyzed_result} -o {scan_tmp_dir}{fileName}'
        run_command_with_timeout(cmd2, 600, cmd_log_path)
        print(f'{fileName} ort scanner finished.')

        # report
        scanned_result = f'{scan_tmp_dir}{fileName}/scan-result.yml'
        cmd3 = f'{ort_tool_path} report -i {scanned_result} -o {report_tmp_dir}{fileName} -f SpdxDocument,CycloneDx'
        run_command_with_timeout(cmd3, 600, cmd_log_path)
        print(f'{fileName} ort reporter finished.\n')

        cdx_report = f'{report_tmp_dir}{fileName}/bom.cyclonedx.xml'
        file_cdx = outputdir + 'cdx#ort#' + fileName + '.xml'
        cp_cdx = f'cp {cdx_report} {file_cdx}'

        spdx_report = f'{report_tmp_dir}{fileName}/bom.spdx.yml'
        file_spdx = outputdir + 'spdx#ort#' + fileName + '.yml'
        cp_spdx = f'cp {spdx_report} {file_spdx}'

        run_command_with_timeout(cp_cdx, 5)
        run_command_with_timeout(cp_spdx, 5)


read_dir_run()
