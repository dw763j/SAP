import os
import csv

tools_spdx = ['syft', 'gh-sbom', 'ort', 'sbom-tool']
tools_cdx = ['syft', 'gh-sbom', 'ort', 'scancode', 'cdxgen']

spdx_head = ['repo_name', 'files_num1', 'files_num2', 'matched_files', 'pkgs_num1', 'pkgs_num2', 'matched_pkgs', 'doc_name', 'originator_score_r', 'supplier_score_r', 'copyright_score_r', 'version_score_r', 'PVC_score_r', 'cpe_score_r',
             'purl_score_r', 'dL_score_r', 'licenseC_score_r', 'licenseD_score_r', 'originator_score', 'supplier_score', 'copyright_score', 'version_score', 'PVC_score', 'cpe_score', 'purl_score', 'dL_score', 'licenseC_score', 'licenseD_score', 'checksum_score']
cdx_head = ['repo_name', 'repo_name_meta', 'repo_version', 'comp_num1', 'comp_num2', 'matched_comps',
            'author_score', 'type_score', 'purl_score', 'cpe_score', 'license_score', 'version_score']

# input_path = '/mnt/three-lans/parse_results/consistency_zero-c-cpp/'
# output_path = '/mnt/three-lans/parse_results/consistency-c-cpp/'


def run_consistency_analyzer(input_path, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with open(os.path.join(output_path, 'consistency-spdx-package.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tools'] + spdx_head[6:])

    with open(os.path.join(output_path, 'consistency-cdx-package.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tools'] + cdx_head[1:3] + cdx_head[5:])

    for i in range(len(tools_spdx)):
        for j in range(i+1, len(tools_spdx)):
            spdx_data = []
            path = os.path.join(input_path, f'spdx-{tools_spdx[i]}-{tools_spdx[j]}-package-consistency.csv')
            with open(path, 'r') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    spdx_data_row = []
                    max_pkg = max(int(row[4]), int(row[5]))
                    if max_pkg == 0:
                        spdx_data_row += [0]
                    else:
                        spdx_data_row += [int(row[6]) / max_pkg]  # row[6] is matched_pkgs
                    spdx_data_row += row[7:-1]  # information of the pkg that pairs on the reponame
                    matched_files = float(row[3])
                    if matched_files == 0:
                        spdx_data_row += [0]
                    else:
                        spdx_data_row += [float(row[-1]) / matched_files]  # checksum_score
                    spdx_data.append([float(x) for x in spdx_data_row])
                print([sum(x) / len(x) for x in zip(*spdx_data)])
            with open(os.path.join(output_path, 'consistency-spdx-package.csv'), 'a') as file:
                writer = csv.writer(file)
                writer.writerow([f'{tools_spdx[i]}+{tools_spdx[j]}'] + [round(sum(x) / len(x), 4) for x in zip(*spdx_data)])

    for i in range(len(tools_cdx)):
        for j in range(i+1, len(tools_cdx)):
            cdx_data = []
            path = os.path.join(input_path, f'cdx-{tools_cdx[i]}-{tools_cdx[j]}-package-consistency.csv')
            with open(path, 'r') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cdx_data_row = row[1:3]
                    max_pkg = max(int(row[3]), int(row[4]))
                    if max_pkg == 0:
                        cdx_data_row += [0]
                    else:
                        cdx_data_row += [int(row[5]) / max_pkg]
                    cdx_data_row += row[6:]
                    cdx_data.append([float(x) for x in cdx_data_row])
            with open(os.path.join(output_path, 'consistency-cdx-package.csv'), 'a') as file:
                writer = csv.writer(file)
                writer.writerow([f'{tools_cdx[i]}+{tools_cdx[j]}'] + [round(sum(x) / len(x), 4) for x in zip(*cdx_data)])
