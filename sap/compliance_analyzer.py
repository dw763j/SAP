import csv
import os
# cdx: # tool,repo,bomFormat_exist,bomFormat_true_exist,specVersion_exist,specVersion_true_exist,version_exist,version_true_exist,
# serialNumber_exist,serialNumber_true_exist,timestamp_exist,timestamp_true_exist,tools_exist,tools_true_exist,
# name_com_exist,name_com_true_exist,version_com,version_com_true_exist,bom_ref_com_exist,bom_ref_com_true_exist,
# names_exist,names_true_exist,author_exist,author_true_exist,type_exist,type_true_exist,
# bom_ref_exist,bom_ref_true_exist,purl_exist,purl_true_exist,cpe_exist,cpe_true_exist,
# licenses_exist,licenses_true_exist,version_exist,version_true_exist,copyright_exist,copyright_true_exist

data = []
repo = []

tools_spdx = ['syft', 'gh-sbom', 'ort', 'sbom-tool']
tools_cdx = ['syft', 'gh-sbom', 'ort', 'scancode', 'cdxgen']

# lan = 'python'
# input_path = f'/mnt/three-lans/parse_results/field_statistic-{lan}/'
# results_path = f'/mnt/three-lans/parse_results/compliance-{lan}/'


def run_compliance_analyzer(input_path, results_path):

    if not os.path.exists(results_path):
        os.makedirs(results_path)

    print('\n\ncdx rate')
    print('bomFormat,specVersion,version,serialNumber,timestamp,tools,name_com,version_com,bom_ref_com,names,author,type,bom_ref,purl,cpe,licenses,version,copyright')
    cdx_items = ['bomFormat', 'specVersion', 'version', 'serialNumber', 'timestamp', 'tools', 'name_com', 'version_com',
                 'bom_ref_com', 'names', 'author', 'type', 'bom_ref', 'purl', 'cpe', 'licenses', 'version', 'copyright']
    cdx_true_exist_rate = []
    cdx_relate_to_pkgs_rate = []
    for tool in tools_cdx:
        true_exist_rate = []
        relate_to_pkgs_rate = []
        with open(os.path.join(input_path, f'cdx_{tool}_statistic.csv'), 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                true_exist_rate_row = []
                relate_to_pkgs_rate_row = []
                for i in range(2, 38, 2):
                    if row[i] == '0':
                        true_exist_rate_row.append(0)
                    else:
                        true_exist_rate_row.append(int(row[i+1])/int(row[i]))
                    if i >= 20:
                        if row[20] == '0':
                            relate_to_pkgs_rate_row.append(0)
                        else:
                            relate_to_pkgs_rate_row.append(int(row[i+1])/int(row[20]))  # true exist compare with pkgs
                true_exist_rate.append(true_exist_rate_row)
                relate_to_pkgs_rate.append(relate_to_pkgs_rate_row)

        total_true_exist_rate = [tool]+[round(sum(x) / len(x), 4) for x in zip(*true_exist_rate)]
        total_relate_to_pkgs_rate = [tool]+[round(sum(x) / len(x), 4) for x in zip(*relate_to_pkgs_rate)]
        print(tool, 'total_true_exist_rate', total_true_exist_rate)
        print(tool, 'total_relate_to_pkgs_rate', total_relate_to_pkgs_rate)  # from pkg_name
        cdx_true_exist_rate.append(total_true_exist_rate)
        cdx_relate_to_pkgs_rate.append(total_relate_to_pkgs_rate)

    with open(os.path.join(results_path, 'compliance-cdx_true_exist_rate.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tool']+cdx_items)
        writer.writerows(cdx_true_exist_rate)
    with open(os.path.join(results_path, 'compliance-cdx_relate_to_pkgs_rate.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tool']+cdx_items[9:])
        writer.writerows(cdx_relate_to_pkgs_rate)

    # spdx:
    # tool,repo,Name_exist,Name_true_exist,spdxVersion_exist,spdxVersion_true_exist,
    # dataLicense_exist,dataLicense_true_exist,SPDXID_exist,SPDXID_true_exist,documentNamespace_exist,documentNamespace_true_exist,
    # creators_exist,creators_true_exist,created_exist,created_true_exist,name_exist,name_true_exist,
    # spdxid_exist,spdxid_true_exist,downloadLocation_exist,downloadLocation_true_exist,versionInfo_exist,versionInfo_true_exist,
    # packageVerificationCode_exist,packageVerificationCode_true_exist,originator_exist,originator_true_exist,supplier_exist,supplier_true_exist,
    # licenseConcluded_exist,licenseConcluded_true_exist,licenseDeclared_exist,licenseDeclared_true_exist,copyrightText_exist,copyrightText_true_exist,
    # externalRefs_exist,externalRefs_true_exist,cpe_exist,cpe_true_exist,cpe_pkg_level,purl_exist,
    # purl_true_exist,name_exist,name_true_exist,spdxid_exist,spdxid_true_exist,checksum_exist,checksum_true_exist
    spdx_items = ['Name', 'spdxVersion', 'dataLicense', 'SPDXID', 'documentNamespace', 'creators', 'created', 'name', 'spdxid', 'downloadLocation',
                  'versionInfo', 'packageVerificationCode', 'originator', 'supplier', 'licenseConcluded', 'licenseDeclared', 'copyrightText', 'externalRefs', 'cpe', 'purl', 'file_name', 'file_spdxid', 'checksum']
    spdx_true_exist_rate = []
    spdx_relate_to_pkgs_rate = []
    print('\n\nspdx rate')
    print('Name,spdxVersion,dataLicense,SPDXID,documentNamespace,creators,created,name,spdxid,downloadLocation,versionInfo,packageVerificationCode,originator,supplier,licenseConcluded,licenseDeclared,copyrightText,externalRefs,cpe,purl,name,spdxid,checksum')
    for tool in tools_spdx:
        # rate = []
        true_exist_rate = []
        relate_to_pkgs_rate = []
        with open(os.path.join(input_path, f'spdx_{tool}_statistic.csv'), 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                true_exist_rate_row = []
                relate_to_pkgs_rate_row = []
                for i in range(2, 40, 2):
                    if row[i] == '0':
                        true_exist_rate_row.append(0)
                    else:
                        true_exist_rate_row.append(int(row[i+1])/int(row[i]))
                    if i >= 16:
                        if row[16] == '0':
                            relate_to_pkgs_rate_row.append(0)
                        else:
                            relate_to_pkgs_rate_row.append(int(row[i+1])/int(row[16]))  # true exist compare with pkgs
                true_exist_rate.append(true_exist_rate_row)
                relate_to_pkgs_rate.append(relate_to_pkgs_rate_row)
                for i in range(41, 48, 2):
                    if row[i] == '0':
                        true_exist_rate_row.append(0)
                    else:
                        true_exist_rate_row.append(int(row[i+1])/int(row[i]))
                    if row[16] == '0':
                        relate_to_pkgs_rate_row.append(0)
                    else:
                        relate_to_pkgs_rate_row.append(int(row[i+1])/int(row[16]))
                true_exist_rate.append(true_exist_rate_row)
                relate_to_pkgs_rate.append(relate_to_pkgs_rate_row)
                # calculating for data fields compliance rate
        total_true_exist_rate = [tool]+[round(sum(x) / len(x), 4) for x in zip(*true_exist_rate)]
        total_relate_to_pkgs_rate = [tool]+[round(sum(x) / len(x), 4) for x in zip(*relate_to_pkgs_rate)]
        print(tool, 'total_true_exist_rate', total_true_exist_rate)
        print(tool, 'total_relate_to_pkgs_rate', total_relate_to_pkgs_rate)  # from pkg_name
        spdx_true_exist_rate.append(total_true_exist_rate)
        spdx_relate_to_pkgs_rate.append(total_relate_to_pkgs_rate)
    with open(os.path.join(results_path, 'compliance-spdx_true_exist_rate.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tool']+spdx_items)
        writer.writerows(spdx_true_exist_rate)
    with open(os.path.join(results_path, 'compliance-spdx_relate_to_pkgs_rate.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['tool']+spdx_items[7:])
        writer.writerows(spdx_relate_to_pkgs_rate)
