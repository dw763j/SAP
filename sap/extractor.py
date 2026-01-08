import hashlib
import json
import os
from loguru import logger


def log_not_exist_file(filepath):
    logger.error(f'[FileNotExist]: {filepath}')


def extract_packages_info(data, filename):
    # type of data is dict
    # `NE` = NotExist, which means the field doesn't exist.
    # data = json.dumps(data, indent=4)ß
    package_info = {

    }

    for d in data:
        if 'SPDXID' not in d:
            logger.error(f'[NoSPDXID]: {filename}')
            return package_info
        id = d['SPDXID']        # spdxid
        package_info[id] = {
            'name':             d['name'],
            'SPDXID':           id,
            'downloadLocation': d.get('downloadLocation', 'NE'),
            'packageVerificationCode':   d.get('packageVerificationCode', 'NE'),
            'versionInfo':      d.get('versionInfo', 'NE'),
            'originator':       d.get('originator', 'NE'),
            'supplier':         d.get('supplier', 'NE'),
            'licenseConcluded': d.get('licenseConcluded', 'NE'),
            'licenseDeclared':  d.get('licenseDeclared', 'NE'),
            'copyrightText':    d.get('copyrightText', 'NE'),
            'externalRefs':     d.get('externalRefs', 'NE'),
        }

    return package_info


def extract_files_info(data, filename):
    files_info = {}
    for d in data:
        files_info[d['SPDXID']] = {
            'name':      d['fileName'],
            'SPDXID':    d['SPDXID'],
            'checksums': d['checksums']
        }
    return files_info


def get_filename(path):
    string = os.path.basename(path)
    filename = string[:string.rfind(".")]
    return filename


def spdx_field_extract(filepath, wb_path):
    filename = get_filename(filepath)
    spdx_info = {}
    with open(filepath, 'r') as fd:
        data = json.load(fd)
        # Document Creation Information
        create_info = {}
        create_info['name'] = data.get('name', 'NE')
        create_info['spdxVersion'] = data.get('spdxVersion', 'NE')
        create_info['dataLicense'] = data.get('dataLicense', 'NE')
        create_info['SPDXID'] = data.get('SPDXID', 'NE')
        create_info['documentNamespace'] = data.get('documentNamespace', 'NE')
        if 'creationInfo' in data:
            creationInfo = data['creationInfo']
            create_info['creators'] = creationInfo.get('creators', 'NE')
            create_info['created'] = creationInfo.get('created', 'NE')
        else:
            create_info['creators'] = 'NE'
            create_info['created'] = 'NE'
        spdx_info['documents'] = create_info

        # packages information
        packages_info = {}
        if 'packages' in data:
            packages = data['packages']
            packages_info = extract_packages_info(packages, filename)
        else:
            packages_info = 'NE'
            logger.error(f'[NoPackagesField]: {filepath}')
        spdx_info['packages'] = packages_info

        # files information
        if 'files' in data:
            files = data['files']
            files = extract_files_info(files, filename)
        else:
            files = 'NE'
            logger.debug(f'[NoFiles]: {filepath}')
        spdx_info['files'] = files

    spdx_info = json.dumps(spdx_info, indent=4)
    # Statistic
    # wb_path = '/mnt/three-lans/parse_results/field_extraction-python/'
    # wb_path = '/mnt/sbom-final-codes/add_results/field_extraction/'
    # if not os.path.exists(wb_path):
    #     os.makedirs(wb_path)
    wb_file = os.path.join(wb_path, f'{filename}.json')
    with open(wb_file, 'w') as fd:
        fd.writelines(spdx_info)
    logger.success(f'[SPDXExtractionSucceed]: {filename}')


def extract_metadata(data, filename):
    metadata = data.get('metadata', 'NE')
    # tools = metadata.get('tools', 'NE')
    component_in_metadata = metadata.get('component', 'NE')
    if (component_in_metadata == 'NE'):
        component_in_metadata = {}
        logger.error(f'[NoComponentInMetadata]: {filename}')
    metadata_info = {
        # cdx
        'bomFormat':        data.get('bomFormat', 'NE'),
        'specVersion':      data.get('specVersion', 'NE'),
        'version':          data.get('version', 'NE'),
        'serialNumber':     data.get('serialNumber', 'NE'),
        # metadata
        'timestamp':        metadata.get('timestamp', 'NE'),
        'tools':            metadata.get('tools', 'NE'),
        'name_com':         component_in_metadata.get('name', 'NE'),
        'version_com':      component_in_metadata.get('version', 'NE'),
        'bom-ref_com':      component_in_metadata.get('bom-ref', 'NE'),
    }

    return metadata_info


def extract_components(data, filename):
    component_info = {}
    for d in data:
        if ('name' not in d):
            logger.error(f'[NoNameInComponent]: {filename}')
            return component_info
        cdxid = d.get('bom-ref', d.get('purl', hashlib.md5(d['name'].encode()).hexdigest()))
        component_info[cdxid] = {
            'name':     d['name'],
            'author':   d.get('author', 'NE'),
            'type':     d.get('type', 'NE'),
            'bom-ref':  d.get('bom-ref', 'NE'),
            'purl':     d.get('purl', 'NE'),
            'licenses': d.get('licenses', 'NE'),
            'version':  d.get('version', 'NE'),
            'copyright': d.get('copyright', 'NE'),
            'cpe':      d.get('cpe', 'NE'),
        }
    return component_info


def ort_extract_metadata(data, filename):
    metadata = data.get('metadata', 'NE')
    # tools = metadata.get('tools', 'NE')
    # component_in_metadata = metadata.get('component', 'NE')
    component_in_metadata = metadata.get('components', 'NE')
    if (component_in_metadata == 'NE'):
        component_in_metadata = {}
        logger.error(f'[NoComponentInMetadata]: {filename}')
    metadata_info = {
        # cdx
        'bomFormat':        data.get('bomFormat', 'NE'),
        'specVersion':      data.get('@xmlns', 'NE'),       # @xmlns as specVersion
        'version':          data.get('@version', 'NE'),
        'serialNumber':     data.get('@serialNumber', 'NE'),
        # metadata
        'timestamp':        metadata.get('timestamp', 'NE'),
        'tools':            metadata.get('tools', 'NE'),
        'name_com':             component_in_metadata.get('name', 'NE'),
        'version_com':          component_in_metadata.get('version', 'NE'),
        'bom-ref_com':          component_in_metadata.get('bom-ref', 'NE'),
    }

    return metadata_info


def ort_extract_components(data, filename):
    component_info = {}
    comp = data.get('component', 'NE')
    if isinstance(comp, dict):
        data = [comp]
    elif isinstance(comp, list):
        data = comp
    else:
        logger.error(f'[ComponentTYPEError]: {filename}')
    for d in data:
        if ('name' not in d):
            logger.error(f'[NoNameInComponent]: {filename}')
            return component_info
        cdxid = d.get('bom-ref', d.get('purl', hashlib.md5(d['name'].encode()).hexdigest()))
        component_info[cdxid] = {
            'name':     d['name'],
            'author':   d.get('author', 'NE'),
            'type':     d.get('type', 'NE'),
            'bom-ref':  d.get('bom-ref', 'NE'),
            'purl':     d.get('purl', 'NE'),
            'licenses': d.get('licenses', 'NE'),
            'version':  d.get('version', 'NE'),
            'copyright': d.get('copyright', 'NE'),
            'cpe':      d.get('cpe', 'NE'),
        }
    return component_info


def ort_field_extract(filepath, wb_path):

    filename = get_filename(filepath)
    cdx_info = {}

    with open(filepath, 'r') as fd:
        data = json.load(fd)
        data = data.get('bom', 'NE')
        if data == 'NE':
            logger.error(f'[OrtBomNotExist]: {filename}')
            return
        # ort specific

        metadata = ort_extract_metadata(data, filename)
        cdx_info['metadata'] = metadata
        component = data.get('components', 'NE')
        if (component == 'NE'):
            cdx_info['components'] = 'NE'
        else:
            cdx_info['components'] = ort_extract_components(component, filename)
    cdx_info = json.dumps(cdx_info, indent=4)

    # Statistic
    # wb_path = '/mnt/sbom-final-codes/add_results/field_extraction/'
    wb_file = os.path.join(wb_path, f'{filename}.json')

    with open(wb_file, 'w') as fd:
        fd.writelines(cdx_info)
    logger.success(f'[CDXExtractionSucceed]: {filename}')


def cdx_field_extraction(filepath, wb_path):
    filename = get_filename(filepath)
    cdx_info = {}

    with open(filepath, 'r') as fd:
        data = json.load(fd)
        metadata = extract_metadata(data, filename)
        cdx_info['metadata'] = metadata
        component = data.get('components', 'NE')
        if (component == 'NE'):
            cdx_info['components'] = 'NE'
        else:
            cdx_info['components'] = extract_components(component, filename)
    cdx_info = json.dumps(cdx_info, indent=4)

    # Statistic
    # wb_path = '/mnt/three-lans/parse_results/field_extraction-python/'
    # if not os.path.exists(wb_path):
    # os.makedirs(wb_path)
    wb_file = os.path.join(wb_path, f'{filename}.json')

    with open(wb_file, 'w') as fd:
        fd.writelines(cdx_info)
    logger.success(f'[CDXExtractionSucceed]: {filename}')


def is_valid_json(file_path):
    if not os.path.exists(file_path):
        return False
    if not file_path.endswith('.json'):
        return False
    try:
        with open(file_path, 'r') as f:
            json.load(f)
        return True
    except json.JSONDecodeError:
        return False


def run_extract(root_sboms_path='', wb_path='', lans=['c-cpp', 'java', 'python']):
    for lan in lans:
        # logger.add(f'/mnt/three-lans/parse_sboms-logs/extract_field-{lan}.log')
        # root_sboms_path = f'/mnt/three-lans/results-{lan}/'
        # root_sboms_filelist = f'/mnt/three-lans/sboms_filelist-{lan}.txt'
        # wb_path = f'/mnt/three-lans/parse_results/field_extraction-{lan}/'
        # extracted_filelist = f'/mnt/three-lans/parse_results/extracted_filelist-{lan}.txt'
        wb_path = os.path.join(wb_path, f'field_extraction-{lan}')
        extracted_filelist = os.path.join(wb_path, f'extracted_filelist-{lan}.txt')

        if not os.path.exists(wb_path):
            os.makedirs(wb_path)
        files = os.listdir(root_sboms_path)

        import concurrent.futures

        def process_file(line):
            filepath = os.path.join(root_sboms_path, line)
            if not os.path.exists(filepath):
                logger.debug(f'[NotExistFile]: {filepath}')
                return
            # if not '#ort#' in line:  # for ort
            #     return
            if not is_valid_json(filepath):
                logger.error(f'[Invalid JSON File]: {filepath}')
            elif 'spdx' in line:
                spdx_field_extract(filepath, wb_path)
            elif 'cdx#ort' in line:
                ort_field_extract(filepath, wb_path)
            elif 'cdx' in line:
                cdx_field_extraction(filepath, wb_path)
            else:
                logger.debug(f'[NotSBOM]: {line}')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_file, files)

        with open(extracted_filelist, 'w') as fd:
            for file in os.listdir(wb_path):
                fd.write(file + '\n')
        return extracted_filelist, wb_path


if __name__ == '__main__':
    run_extract()
