import os
import csv
tools_spdx = ['syft', 'gh-sbom', 'ort', 'sbom-tool']
tools_cdx = ['syft', 'gh-sbom', 'ort', 'scancode', 'cdxgen']
# spdx_head = ['repo_name', 'files_num1', 'files_num2', 'matched_files', 'pkgs_num1', 'pkgs_num2', 'matched_pkgs', 'doc_name', 'originator_score_r', 'supplier_score_r', 'copyright_score_r', 'version_score_r', 'PVC_score_r', 'cpe_score_r',
#              'purl_score_r', 'dL_score_r', 'licenseC_score_r', 'licenseD_score_r', 'originator_score', 'supplier_score', 'copyright_score', 'version_score', 'PVC_score', 'cpe_score', 'purl_score', 'dL_score', 'licenseC_score', 'licenseD_score', 'checksum_score']
# cdx_head = ['repo_name', 'repo_name_meta', 'repo_version', 'comp_num1', 'comp_num2', 'matched_comps',
#             'author_score', 'type_score', 'purl_score', 'cpe_score', 'license_score', 'version_score']
cdx_head = ['repo_name', 'repo_name_meta', 'repo_version', 'comp_num', 'comp_bench', 'matched_comps',
            'author_score', 'license_score', 'version_score']

spdx_head = ['repo_name', 'pkgs_num', 'pkgs_bench', 'matched_pkgs',
             # repo
             'doc_name', 'originator_score_r', 'supplier_score_r', 'copyright_score_r', 'version_score_r',
             'dL_score_r', 'licenseC_score_r', 'licenseD_score_r',
             # pkg
             'originator_score', 'supplier_score', 'version_score', 'dL_score', 'licenseC_score', 'licenseD_score']

# input_path = '/mnt/three-lans/parse_results/accuracy-python/'
# output_path = '/mnt/three-lans/parse_results/accuracy-python/'


def run_accuracy_analyzer(input_path, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with open(os.path.join(output_path, 'accuracy-spdx.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tools', 'precision', 'recall'] + spdx_head[4:] + ['count'])

    with open(os.path.join(output_path, 'accuracy-cdx.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tools', 'precision', 'recall'] + cdx_head[1:3] + cdx_head[6:] + ['count'])

    for i in range(len(tools_spdx)):
        path = os.path.join(input_path, f'spdx-{tools_spdx[i]}-accuracy.csv')
        spdx_data = []
        count = 0
        with open(path, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                count += 1
                spdx_data_row = []
                matched_pkg = int(row[3])
                if matched_pkg == 0:
                    spdx_data_row += [0, 0]
                else:
                    spdx_data_row += [int(row[3]) / int(row[1]), int(row[3]) / int(row[2])]  # precision, recall
                spdx_data_row += row[4:]  # information of the pkg that pairs on the reponame
                spdx_data.append([float(x) for x in spdx_data_row])
            print([sum(x) / len(x) for x in zip(*spdx_data)], '\n')
        with open(os.path.join(output_path, 'accuracy-spdx.csv'), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([f'{tools_spdx[i]}'] + [round(sum(x) / len(x), 4) for x in zip(*spdx_data)] + [count])

    for i in range(len(tools_cdx)):
        cdx_data = []
        count = 0
        path = os.path.join(input_path, f'cdx-{tools_cdx[i]}-accuracy.csv')
        with open(path, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                count += 1
                cdx_data_row = []
                matched_pkg = int(row[5])
                if matched_pkg == 0:
                    cdx_data_row += [0, 0]
                else:
                    cdx_data_row += [int(row[5]) / int(row[3]), int(row[5]) / int(row[4])]  # precision, recall
                cdx_data_row += row[1:3]
                cdx_data_row += row[6:]
                cdx_data.append([float(x) for x in cdx_data_row])
            print([sum(x) / len(x) for x in zip(*cdx_data)])
        with open(os.path.join(output_path, 'accuracy-cdx.csv'), 'a') as file:
            writer = csv.writer(file)
            writer.writerow([f'{tools_cdx[i]}'] + [round(sum(x) / len(x), 4) for x in zip(*cdx_data)] + [count])
