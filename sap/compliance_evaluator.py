import json
import os
import csv
from loguru import logger


def split_filename2tool_repo(filename):
    tool = filename.split("#")[-2]
    repo = filename.split("#")[-1].split(".")[0]
    return tool, repo


def write_row2csv(filename: str, data: list) -> None:
    with open(filename, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(data)


def is_valid_json(file_path):
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, 'r') as f:
            json.load(f)
        return True
    except json.JSONDecodeError:
        return False


def valid_value(v):
    if v is None or v == 'NE' or v == 'NONE' or v == 'NOASSERTION' or v == 'null':
        return False
    return True


def external_ref_proc(externalRefs: list) -> list:
    cpe_list = []
    purl_list = []
    cpe_all = 0
    if isinstance(externalRefs[0], list) and len(externalRefs) == 1:
        externalRefs = externalRefs[0]
    for e in externalRefs:
        if isinstance(e, str) and e != 'NE':
            logger.debug(f'[ExternalRef]: {e}')
            continue
        if isinstance(e, list):
            ee_cpe_flag = False
            for ee in e:
                ref_type = ee.get('referenceType', "NE")
                if 'cpe' in ref_type:
                    ee_cpe_flag = True
                    cpe_list.append(ee['referenceLocator'])
                elif 'purl' in ref_type:
                    purl_list.append(ee['referenceLocator'])
            if ee_cpe_flag:
                cpe_all += 1
        elif isinstance(e, dict):
            ref_type = e.get('referenceType', "NE")
            if 'cpe' in ref_type:
                cpe_all += 1
                cpe_list.append(e['referenceLocator'])
            elif 'purl' in ref_type:
                purl_list.append(e['referenceLocator'])
        elif isinstance(e, str) and e == 'NE':
            continue
        else:
            logger.error(f'[ExternalRefType]: {type(e)}')

    result = [
        sum([int(x != 'NE') for x in cpe_list]), sum([int(valid_value(x)) for x in cpe_list]),
        sum([int(x != 'NE') for x in purl_list]), sum([int(valid_value(x)) for x in purl_list]),
        cpe_all
    ]
    return result


def cdx_metadata_statistic(metadata: dict) -> list:
    # tmp_list = [bomFormat, specVersion, version, serialNumber, timestamp, tools, name_com, version_com, bom_ref_com]
    metadata_item_list = ['bomFormat', 'specVersion', 'version', 'serialNumber', 'timestamp', 'tools', 'name_com', 'version_com', 'bom-ref_com']  # FIXME
    metadata_items = {m: metadata.get(m) for m in metadata_item_list}
    metadata_items['statistic'] = []

    for item in metadata_items:
        if item == 'statistic':
            continue
        metadata_items['statistic'] += [int(metadata_items[item] != 'NE'), int(valid_value(metadata_items[item]))]
    return metadata_items


def cdx_components_statistic(components: dict) -> list:
    components_item_list = ['name', 'author', 'type', 'bom-ref', 'purl', 'cpe', 'licenses', 'version', 'copyright']
    components_items = {c: [] for c in components_item_list}
    for c in components:
        for item in components_item_list:
            components_items[item].append(components[c].get(item, "NE"))

    components_items['statistic'] = []
    for item in components_item_list:
        if item == 'statistic':
            continue
        components_items['statistic'] += [sum([int(x != 'NE') for x in components_items[item]]), sum([int(valid_value(x)) for x in components_items[item]])]

    return components_items


def cdx_statistic(filepath, filename, result_path):
    fp = os.path.join(filepath, filename)
    with open(fp, 'r') as f:
        cdx = json.load(f)
    metadata = cdx.get('metadata', 'NE')
    components = cdx.get('components', 'NE')
    if metadata == 'NE':
        # [bomFormat, specVersion, version, serialNumber, timestamp, tools, name_com, version_com, bom_ref_com]
        metadata_result = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    else:
        metadata_items = cdx_metadata_statistic(metadata)
        metadata_result = metadata_items['statistic']
    if components == 'NE':
        # [name_exist, name_true_exist,author_exist, author_true_exist,type_exist, type_true_exist,
        # bom_ref_exist, bom_ref_true_exist, purl_exist, purl_true_exist, cpe_exist, cpe_true_exist,
        # licenses_exist, licenses_true_exist, version_exist, version_true_exist]
        components_result = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    else:
        components_items = cdx_components_statistic(components)
        components_result = components_items['statistic']

    tool, repo = split_filename2tool_repo(filename)
    basic_info = [tool, repo]

    result = basic_info + metadata_result + components_result
    write_row2csv(os.path.join(result_path, f'cdx_{tool}_statistic.csv'), result)
    logger.info(f'[CDXSuccess]: {filename}')


def spdx_document_statistic(document: dict) -> list:
    # result = []
    document_item_list = ['name', 'spdxVersion', 'dataLicense', 'SPDXID', 'documentNamespace', 'creators', 'created']
    document_items = {d: document.get(d, 'NE') for d in document_item_list}
    document_items['statistic'] = []
    for item in document_items:
        if item == 'statistic':
            continue
        document_items['statistic'] += [int(document_items[item] != 'NE'), int(valid_value(document_items[item]))]
    return document_items


def spdx_packages_statistic(packages: dict) -> list:
    packages_item_list = ['name', 'SPDXID', 'downloadLocation', 'versionInfo', 'packageVerificationCode',
                          'originator', 'supplier', 'licenseConcluded', 'licenseDeclared', 'copyrightText', 'externalRefs']
    packages_items = {p: [] for p in packages_item_list}

    for p in packages:
        for item in packages_item_list:
            packages_items[item].append(packages[p].get(item, "NE"))

    packages_items['statistic'] = []
    for item in packages_item_list:
        if item == 'statistic':
            continue
        elif item == 'externalRefs':
            exf, exf_true = sum([int(x == 'NE') for x in packages_items[item]]), sum([int(valid_value(x)) for x in packages_items[item]])
            if exf_true:
                cpe, cpe_true, purl, purl_true, cpe_all = external_ref_proc(packages_items[item])
            else:
                cpe, cpe_true, purl, purl_true, cpe_all = 0, 0, 0, 0, 0
            packages_items['statistic'] += [exf, exf_true, cpe, cpe_true, cpe_all, purl, purl_true]
        else:
            packages_items['statistic'] += [sum([int(x != 'NE') for x in packages_items[item]]), sum([int(valid_value(x)) for x in packages_items[item]])]
    return packages_items


def sodx_files_statistic(files: dict) -> list:
    files_item_list = ['name', 'SPDXID', 'checksums']
    files_items = {f: [] for f in files_item_list}
    for f in files:
        for item in files_item_list:
            files_items[item].append(files[f].get(item, "NE"))
    files_items['statistic'] = []
    for item in files_item_list:
        if item == 'statistic':
            continue
        files_items['statistic'] += [sum([int(x != 'NE') for x in files_items[item]]), sum([int(valid_value(x)) for x in files_items[item]])]
    return files_items


def spdx_statistic(filepath, filename, result_path):
    fp = os.path.join(filepath, filename)
    with open(fp, 'r') as f:
        spdx = json.load(f)
    document = spdx.get('documents', 'NE')
    packages = spdx.get('packages', 'NE')
    files = spdx.get('files', 'NE')
    if document == 'NE':
        document_result = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    else:
        document_items = spdx_document_statistic(document)
        document_result = document_items['statistic']
    if packages == 'NE':
        packages_result = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    else:
        packages_items = spdx_packages_statistic(packages)
        packages_result = packages_items['statistic']
    if files == 'NE':
        files_result = [0, 0, 0, 0, 0, 0]
    else:
        files_items = sodx_files_statistic(files)
        files_result = files_items['statistic']

    tool, repo = split_filename2tool_repo(filename)
    basic_info = [tool, repo]
    result = basic_info + document_result + packages_result + files_result
    write_row2csv(os.path.join(result_path, f'spdx_{tool}_statistic.csv'), result)
    logger.info(f'[SPDXSuccess]: {filename}')


def run_compliance_evaluator(filepath, extracted_filelist, result_path):
    cdx_tools = ['ort', 'syft', 'gh-sbom', 'cdxgen', 'scancode']
    spdx_tools = ['ort', 'syft', 'gh-sbom', 'sbom-tool']
    # lans = ['c-cpp', 'java', 'python']
    result_path = os.path.join(result_path, 'compliance')

    # need to change the language.
    # filepath = '/mnt/three-lans/parse_results/field_extraction-python/'
    # extracted_filelist = '/mnt/three-lans/parse_results/extracted_filelist-python.txt'
    # result_path = '/mnt/three-lans/parse_results/field_statistic-python/'

    if not os.path.exists(result_path):
        os.makedirs(result_path)

    # logger.add("/mnt/three-lans/parse_sboms-logs/field_statistic-python.log")

    for c in cdx_tools:
        with open(os.path.join(result_path, f'cdx_{c}_statistic.csv'), 'w') as fd:
            writer = csv.writer(fd)
            writer.writerow([
                'tool', 'repo',
                # metadata
                'bomFormat_exist', 'bomFormat_true_exist',
                'specVersion_exist', 'specVersion_true_exist', 'version_exist',
                'version_true_exist', 'serialNumber_exist', 'serialNumber_true_exist',
                'timestamp_exist', 'timestamp_true_exist', 'tools_exist', 'tools_true_exist',
                'name_com_exist', 'name_com_true_exist', 'version_com', 'version_com_true_exist',
                'bom_ref_com_exist', 'bom_ref_com_true_exist',
                # components
                'names_exist', 'names_true_exist', 'author_exist', 'author_true_exist',
                'type_exist', 'type_true_exist', 'bom_ref_exist', 'bom_ref_true_exist',
                'purl_exist', 'purl_true_exist', 'cpe_exist', 'cpe_true_exist',
                'licenses_exist', 'licenses_true_exist',  'version_exist', 'version_true_exist',
                'copyright_exist', 'copyright_true_exist'

            ])
    for s in spdx_tools:
        with open(os.path.join(result_path, f'spdx_{s}_statistic.csv'), 'w') as fd:
            writer = csv.writer(fd)
            writer.writerow([
                'tool', 'repo', 'Name_exist', 'Name_true_exist',
                'spdxVersion_exist', 'spdxVersion_true_exist', 'dataLicense_exist',
                'dataLicense_true_exist', 'SPDXID_exist', 'SPDXID_true_exist',
                'documentNamespace_exist', 'documentNamespace_true_exist', 'creators_exist',
                'creators_true_exist', 'created_exist', 'created_true_exist',
                # packages
                'name_exist', 'name_true_exist', 'spdxid_exist', 'spdxid_true_exist',
                'downloadLocation_exist', 'downloadLocation_true_exist',
                'versionInfo_exist', 'versionInfo_true_exist',
                'packageVerificationCode_exist', 'packageVerificationCode_true_exist',
                'originator_exist', 'originator_true_exist', 'supplier_exist', 'supplier_true_exist',
                'licenseConcluded_exist', 'licenseConcluded_true_exist', 'licenseDeclared_exist',
                'licenseDeclared_true_exist', 'copyrightText_exist', 'copyrightText_true_exist',
                'externalRefs_exist', 'externalRefs_true_exist',
                'cpe_exist', 'cpe_true_exist', 'cpe_pkg_level', 'purl_exist', 'purl_true_exist',
                # files
                'name_exist', 'name_true_exist', 'spdxid_exist', 'spdxid_true_exist',
                'checksum_exist', 'checksum_true_exist'
            ])

    # filepath = '/mnt/sbom-final-codes/results/field_extraction/'
    # with open('/mnt/sbom-final-codes/field-extract/extracted_sbom.txt', 'r') as fd:

    with open(extracted_filelist, 'r') as fd:
        line = fd.readline()
        while line:
            if (line == '\n'):
                break
            filename = line.replace('\n', '')
            if 'cdx' in filename:
                cdx_statistic(filepath, filename, result_path)
            elif 'spdx' in filename:
                spdx_statistic(filepath, filename, result_path)
            else:
                logger.error(f'[UnknownFile]: {filename}')
            line = fd.readline()
    return result_path


if __name__ == '__main__':
    run_compliance_evaluator()
