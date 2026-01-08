
import os
import json
import xmltodict
import yaml
from loguru import logger


def is_empty(file):
    with open(file, 'r') as f:
        content = f.read()
    if content:
        return False
    else:
        return True


def xml2json(xml_file, json_file):
    if (is_empty(xml_file)):
        logger.error(f'[EMPTY_FILE]: {xml_file}')
        return
    with open(xml_file, 'r') as f:
        xml = f.read()
    try:
        xmldict = xmltodict.parse(xml)
        jsonxml = json.dumps(xmldict, indent=4)
        logger.info(f'[Converted]: {xml_file}')
    except Exception as e:
        logger.error(f'[XML_PARSE_ERROR]: {xml_file}')
        logger.error(e)
        return

    with open(json_file, 'w') as f:
        f.write(jsonxml)


def yml2json(yml_file, json_file):
    if (is_empty(yml_file)):
        logger.error(f'[EMPTY_FILE]: {yml_file}')
        return
    with open(yml_file, 'r') as f:
        yml = f.read()
    try:
        ymldict = yaml.load(yml, Loader=yaml.FullLoader)
        ymljson = json.dumps(ymldict, indent=4)
        logger.info(f'[Converted]: {yml_file}')
    except Exception as e:
        logger.error(f'[YML_PARSE_ERROR]: {yml_file}')
        logger.error(e)
        return

    with open(json_file, 'w') as f:
        f.write(ymljson)


def file_extension(filename):
    if filename.endswith('.xml'):
        return filename.replace('.xml', '.json')
    elif filename.endswith('.yml'):
        return filename.replace('.yml', '.json')
    else:
        return filename


def convert(in_path, filename, out_path):
    file = in_path + filename
    outfilr = out_path + file_extension(filename)
    if filename.endswith('.xml'):
        xml2json(file, outfilr)
    elif filename.endswith('.yml'):
        yml2json(file, outfilr)
    else:
        logger.error(f'Unsupported file type: {filename}')


def run():
    for lan in ['java', 'python']:
        import concurrent.futures

        logger.add(f'/mnt/three-lans/parse_sboms-logs/ort-35.0.0-real-ml2json-{lan}.log', format="{time} {level} {message}", level="INFO")
        in_path = f'/mnt/three-lans/ort-35.0.0-results-{lan}-real/sbomfiles/'
        out_path = f'/mnt/three-lans/results-{lan}/'
        if not os.path.exists(out_path):
            os.makedirs(out_path)

        files = os.listdir(in_path)

        def process_file(file):
            convert(in_path, file, out_path)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_file, files)


run()
