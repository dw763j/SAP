import csv
import json
import os
import re
import math
from loguru import logger
from Levenshtein import jaro
from urllib.parse import unquote
import semver
from packageurl import PackageURL
# for debug
fileinvalid = 0
varchar = 0
backspace = 0
VersionNotMatch = 0
NoneOrEmpty = 0
SpecialChar = 0
ManualVersion = 0
# for usage
TotalMatchedVersion = 0
TotalMatchedName = 0


def check_empty(v):
    return v == 'NONE' or v == 'NOASSERTION' or v is None


def equal_cmp(v1, v2):
    if check_empty(v1) and check_empty(v2) or v1 == '' and v2 == '' or check_empty(v1) and v2 == '':
        return 1
    if v1 == 'NE' or v2 == 'NE' or check_empty(v1) or check_empty(v2) or v1 == '' or v2 == '':
        return 0
    if v1.lower() == v2.lower():
        return 1
    else:
        return 0


def check_digit(version):
    return all(char.isdigit() or char == '.' for char in version) and version != ''


def deal_filename(name):
    if name.startswith('./'):
        name = name[2:]
    elif name.startswith('/'):
        name = name[1:]
    return name


def compareName(name1, name2):
    name1, name2 = unquote(name1), unquote(name2)  # fix encoding problem
    name1 = re.sub("pub:|npm:|pip:|go:|actions:|composer:|rust:|ruby:|nuget:|rubygems:|docker:|maven:|iconv:| ", '', name1).lower()
    name2 = re.sub("pub:|npm:|pip:|go:|actions:|composer:|rust:|ruby:|nuget:|rubygems:|docker:|maven:|iconv:| ", '', name2).lower()

    return name1 == name2


def purl_consistency(p1, p2):
    if p1 and p2:
        if p1.lower() == p2.lower():
            return 1.
    if p1 == 'NE' or p2 == 'NE':
        return 0.
    if check_empty(p1) or check_empty(p2):
        return 0.
    try:
        p1_dict = PackageURL.from_string(p1).to_dict()
        p2_dict = PackageURL.from_string(p2).to_dict()
    except Exception:
        # logger.error(f'Invalid purl: {p1}||{p2}')
        return jaro(p1.lower(), p2.lower())
    score = 0
    try:
        for key in p1_dict.keys():
            if key == 'version':
                score += version_consistency(p1_dict[key] if p1_dict[key] is not None else '', p2_dict[key] if p2_dict[key] is not None else '')
            else:
                score += jaro(str(p1_dict[key]).lower() if p1_dict[key] is not None else '', str(p2_dict[key]).lower() if p2_dict[key] is not None else '')
        score = score / len(p1_dict.keys())
    except Exception as e:
        logger.error(f'ERROR {e} at purl_consistency: {p1}||{p2}')
        return jaro(p1.lower(), p2.lower())
    return score


def longest_common_substring_consistency_score(str1, str2):
    if check_empty(str1) and check_empty(str2) or str1 == '' and str2 == '' or check_empty(str1) and str2 == '':
        return 1
    if str1 == 'NE' or str2 == 'NE' or check_empty(str1) or check_empty(str2) or str1 == '' or str2 == '':
        return 0.
    if str1 == str2:
        return 1.
    if isinstance(str1, str)or isinstance(str2, str):
        logger.error(f'Invalid string: {str1}||{str2}')

    str1, str2 = unquote(str1), unquote(str2)  # fix encoding problem
    # Create a 2D array to store lengths of longest common suffixes
    dp = [[0] * (len(str2) + 1) for _ in range(len(str1) + 1)]
    longest_len = 0

    # Build the dp table and find the longest length
    for i in range(1, len(str1) + 1):
        for j in range(1, len(str2) + 1):
            if str1[i-1] == str2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
                if dp[i][j] > longest_len:
                    longest_len = dp[i][j]
            else:
                dp[i][j] = 0

    # Extract the longest common substring
    # longest_common_substr = str1[end_pos-longest_len:end_pos]
    consistency_score = longest_len / max(len(str1), len(str2)) if max(len(str1), len(str2)) else 0.
    return consistency_score


def version_consistency(version1, version2):
    global SpecialChar, fileinvalid, varchar, backspace, VersionNotMatch, NoneOrEmpty, ManualVersion
    if check_empty(version1) and check_empty(version2):  # FIXME
        # logger.error(f'[EmptyVersionValue]: {version1}||{version2}')
        return 1.
    if check_empty(version1) or check_empty(version2) or version1 == 'NE' or version2 == 'NE':  # FIXME
        return 0.
    if version1.lower() == version2.lower():
        return 1.
    if version1 == "" or version2 == "":
        return 0.
    if '15.4.6' in version1 or '15.4.6' in version2:
        logger.info(f'[ManualDefinedVersion]: {version1}||{version2}')
        ManualVersion += 1
        return 0.  # manually defined version in repo
    weights = [0.7, 0.2, 0.1]

    version1 = version1.strip().replace(' ', '')
    version2 = version2.strip().replace(' ', '')
    if version1.startswith('v') or version1.startswith('V'):
        version1 = version1[1:]
    if version2.startswith('v') or version2.startswith('V'):
        version2 = version2[1:]

    if version1.lower() == version2.lower():
        return 1.

    score = 0
    special = ['<', '>', '=', '+', ',', '~', '!', '-']
    for sp in special:
        if sp in version1 or sp in version2:
            SpecialChar += 1
            logger.info(f'[SpecialChar]: {version1}||{version2}')
            break
    try:
        if semver.Version.is_valid(version1) and semver.Version.is_valid(version2):
            semver_version1 = semver.Version(version1)
            semver_version2 = semver.Version(version2)
            count_1 = 0
            count_2 = 0
            for i in semver_version1:
                if i is not None:
                    count_1 += 1
            for i in semver_version2:
                if i is not None:
                    count_2 += 1
            count = min(count_1, count_2)
            if count == 4:
                weights = [0.6, 0.2, 0.1, 0.1]
            elif count == 5:
                weights = [0.5, 0.2, 0.15, 0.1, 0.05]
            for i in range(count):
                jaro_score = jaro(semver_version1[i], semver_version2[i])
                score += jaro_score*weights[i]
                if score == 1.0:
                    return 1.
                if jaro_score != 1.0:
                    break
            return score
    except Exception:
        pass

    v1_parts = version1.split('.')
    v2_parts = version2.split('.')
    length = min(len(v1_parts), len(v2_parts))
    if length == 2:
        weights = [0.8, 0.2]
    if length == 1:
        weights = [1.]
    length = length if length <= 3 else 3
    diffs = [0., 0., 0.]
    for i in range(length):
        run_flag = False
        if i == 0:
            run_flag = True
        elif diffs[i-1] == weights[i-1]:
            run_flag = True
        if run_flag:
            if v1_parts[i] == v2_parts[i]:
                diffs[i] = weights[i]
            elif (check_digit(v1_parts[i]) and check_digit(v2_parts[i])):
                diffs[i] = math.fabs(int(v1_parts[i]) - int(v2_parts[i])) / max(int(v1_parts[i]), int(v2_parts[i])) * weights[i]
            else:
                diffs[i] = jaro(v1_parts[i].lower(), v2_parts[i].lower()) * weights[i]
    return sum(diffs)


def is_valid_json(file_path):
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, 'r') as f:
            json.load(f)
        return True
    except json.JSONDecodeError:
        return False


def text_consistency(text1, text2):  # for author, originator, supplier, copyright, etc.
    if check_empty(text1) and check_empty(text2) or text1 == '' and text2 == '' or check_empty(text1) and text2 == '':
        # logger.error(f'[EmptyTextValue]: {text1}||{text2}')
        return 1
    if text1 == 'NE' or text2 == 'NE' or check_empty(text1) or check_empty(text2):
        return 0.
    else:
        if text1 == text2:
            return 1.
        return jaro(unquote(text1).lower(), unquote(text2).lower())


def deal_license(license):
    if license == 'NE' or license == '' or check_empty(license):
        return []
    if isinstance(license, str):
        license_list = license.split(' ')
        if len(license_list) > 1:
            if 'AND' in license_list:
                license_list.remove('AND')
            if 'OR' in license_list:
                license_list.remove('OR')
        return license_list
    elif isinstance(license, list):
        license_list = []
        for li in license:
            license_list += deal_license(li)
        return license_list
    elif isinstance(license, dict):
        if 'expression' in license:  # scancode
            license_list = license['expression'].split(' ')
            if len(license_list) > 1:
                if 'AND' in license_list:
                    license_list.remove('AND')
                if 'OR' in license_list:
                    license_list.remove('OR')
        elif 'license' in license:  # ort
            license_list = []
            for x in license['license']:
                if isinstance(x, str):
                    if x == 'id' or x == 'name':
                        license_list.append(license['license'][x])
                    continue
                if 'id' in x:
                    license_list.append(x['id'])
                elif 'name' in x:
                    license_list.append(x['name'])
        return license_list
    else:
        logger.error(f'Invalid license: {license}')
        return []


def license_consistency(license1, license2):
    if check_empty(license1) and check_empty(license2) or license1 == '' and license2 == '' or check_empty(license1) and license2 == '':
        # logger.error(f'[EmptyLicenseValue]: {license1}||{license2}')
        return 1.
    if license1 == 'NE' or license2 == 'NE' or check_empty(license1) or check_empty(license2) or license1 == '' or license2 == '':
        return 0.
    if license1 == license2:
        return 1.

    score = 0.
    license_list1 = deal_license(license1)
    license_list2 = deal_license(license2)
    if type(license_list1) is not list or type(license_list2) is not list:
        logger.error(f'Invalid license deal: {license1}||{license2}||{license_list1}||{license_list2}')

    for l1 in license_list1:
        for l2 in license_list2:
            score += int(l1 == l2)
    score /= max(len(license_list1), len(license_list2))
    return score


def best_triple_match(pkg1, pkgs2, threshold=1):
    best_score = -1
    best_k2 = None
    for k2, pkg2 in pkgs2.items():
        name_score = compareName(pkg1.get('name'), pkg2.get('name'))
        if name_score == 1:
            version1 = pkg1.get('version') if 'version' in pkg1 else pkg1.get('versionInfo')
            version2 = pkg2.get('version') if 'version' in pkg2 else pkg2.get('versionInfo')
            # purl1 = pkg1.get('purl', None)
            # purl2 = pkg2.get('purl', None)
            version_score = version_consistency(version1, version2)
            # if purl1 is None and purl2 is None:
            #     threshold = 1.0
            # purl_score = purl_consistency(purl1, purl2)
            # triple_score = name_score + version_score + purl_score
            triple_score = name_score + version_score
            if triple_score > best_score:
                best_score = triple_score
                best_k2 = k2
        if best_score == 2.0:
            break
    if best_score >= threshold:
        return best_k2, best_score
    else:
        return None, 0


def cdx_consistency(file1_path, file2_path, result_path):
    filename1, tool1, reponame1, filedata1 = parse_fileinfo(file1_path)
    reponame2, filedata2 = parse_fileinfo(file2_path)
    metadata1 = filedata1['metadata']
    component1, component2 = filedata1['components'], filedata2['packages']
    cmp_flag = True
    if component1 == 'NE' or len(component2) == 1 and component2[0] == {}:
        cmp_flag = False
    else:
        component_keys1 = component1.keys()
    all_matched_scores = {}
    all_matched_scores['basic_info'] = f'cdx_{tool1}_{reponame1}'
    if metadata1 == 'NE':
        all_matched_scores['repo_info'] = [0, 0]
    else:
        repo_name1 = metadata1['name_com']
        repo_name2 = filedata2['name']
        if check_empty(repo_name1) and check_empty(repo_name2) or repo_name1 == '' and repo_name2 == '':  # FIXME 4.1 fix
            repo_name_score = 0
        elif repo_name1 == 'NE' or repo_name2 == 'NE' or check_empty(repo_name1) or check_empty(repo_name2) or repo_name1 == '' or repo_name2 == '':  # FIXME
            repo_name_score = 0
        else:
            repo_name_score = jaro(repo_name1.lower(), repo_name2.lower())
        all_matched_scores['repo_info'] = [repo_name_score,
                                           version_consistency(metadata1['version_com'], filedata2['version'])]
    all_matched_scores['pkg_info'] = []
    all_matched_scores['statistic_info'] = []
    matched_pkg = []
    if not cmp_flag:
        all_matched_scores['statistic_info'] += [0, 0, 0]
        all_matched_scores['pkg_info'] = [[0, 0, 0]]
        return all_matched_scores

    # Convert component2 list to dict for best_triple_match compatibility
    component2_dict = {}
    for i, pkg in enumerate(component2):
        if pkg and isinstance(pkg, dict):
            component2_dict[f"pkg_{i}"] = pkg

    for k1 in component_keys1:
        pkg1 = component1[k1]
        best_k2, best_score = best_triple_match(pkg1, component2_dict, 1)  # Using threshold 2.4 like in consistency
        if best_k2 is not None:
            pkg2 = component2_dict[best_k2]
            matched_pkg.append(k1)
            author_score = text_consistency(pkg1['author'], pkg2['author'])
            license_score = license_consistency(pkg1['licenses'], pkg2['license'])
            version_score = version_consistency(pkg1['version'], pkg2['version'])
            result = [author_score, license_score, version_score]
            all_matched_scores['pkg_info'].append(result)

    if len(all_matched_scores['pkg_info']) == 0:
        all_matched_scores['pkg_info'].append([0, 0, 0])

    all_matched_scores['statistic_info'] += [len(component_keys1), len(component2), len(matched_pkg)]
    logger.success(f'[FinishedCDX]: {tool1}||{reponame1}')
    return all_matched_scores


def external_ref_proc(externalRefs: list) -> list:
    if isinstance(externalRefs, str):
        logger.error(f'[InvalidExternalRefs]: {externalRefs}')
        return [[], []]
    cpe_list = []
    purl_list = []
    for e in externalRefs:
        if isinstance(e, str):
            continue
        ref_type = e.get('referenceType', "NE")
        if 'cpe' in ref_type:
            cpe_list.append(e['referenceLocator'])
        elif 'purl' in ref_type:
            purl_list.append(e['referenceLocator'])
    return cpe_list, purl_list


def deal_PVC(PVC):
    if PVC == 'NE' or check_empty(PVC) or PVC == '':
        return None
    if isinstance(PVC, str):
        return PVC
    elif isinstance(PVC, list):
        if len(PVC) == 1:
            logger.info(f'[ListPVC]: {PVC}||{PVC[0]}')
            return PVC[0]
    elif isinstance(PVC, dict):
        if 'packageVerificationCodeValue' in PVC:
            return PVC['packageVerificationCodeValue']
    logger.error(f'Invalid packageVerificationCode: {PVC}')
    return None


def spdx_consistency(file1_path, file2_path, result_path):
    filename1, tool1, reponame1, filedata1 = parse_fileinfo(file1_path)
    reponame2, filedata2 = parse_fileinfo(file2_path)
    doc1 = filedata1['documents']
    pkgs1, pkgs2 = filedata1['packages'], filedata2['packages']  # benchmark里面是list
    # files1, files2 = filedata1['files'], filedata2['files']

    pkg_flag = True
    if pkgs1 == 'NE' or len(pkgs2) == 1 and pkgs2[0] == {}:
        pkg_flag = False
    else:
        pkgs_keys1 = pkgs1.keys()

    # files_flag = True
    # if files1 == 'NE' or files2 == 'NE':
    #     files_flag = False
    # else:
    #     files_keys1, files_keys2 = files1.keys(), files2.keys()

    all_matched_scores = {}
    all_matched_scores['basic_info'] = f'spdx_{tool1}_{reponame1}'
    all_matched_scores['repo_info'] = []
    all_matched_scores['pkg_info'] = []
    # all_matched_scores['files_info'] = []
    all_matched_scores['statistic_info'] = []

    repo1_flag = False
    # repo2_flag = False

    if not pkg_flag:
        all_matched_scores['statistic_info'] += [0, 0, 0]
        all_matched_scores['repo_info'] = [0, 0, 0, 0, 0, 0, 0, 0]
        all_matched_scores['pkg_info'] = [[0, 0, 0, 0, 0, 0]]
        return all_matched_scores

    # Convert pkgs2 list to dict for best_triple_match compatibility
    pkgs2_dict = {}
    for i, pkg in enumerate(pkgs2):
        if pkg and isinstance(pkg, dict):
            pkgs2_dict[f"pkg_{i}"] = pkg

    matched_pkg = []
    for k1 in pkgs_keys1:
        pkg1 = pkgs1[k1]
        if compareName(pkg1['name'], reponame1):
            repo1_flag = True
            all_matched_scores['repo_info'] = [
                jaro(doc1['name'], filedata2['name']),
                text_consistency(pkg1['originator'], filedata2['author']),
                text_consistency(pkg1['supplier'], filedata2['author']),
                text_consistency(pkg1['copyrightText'], filedata2['copyright']),
                version_consistency(pkg1['versionInfo'], filedata2['version']),
                longest_common_substring_consistency_score(pkg1['downloadLocation'], filedata2['home_page']),
                license_consistency(pkg1['licenseConcluded'], filedata2['license']),
                license_consistency(pkg1['licenseDeclared'], filedata2['license'])
            ]

        best_k2, best_score = best_triple_match(pkg1, pkgs2_dict, 1)
        if best_k2 is not None:
            pkg2 = pkgs2_dict[best_k2]
            matched_pkg.append(k1)

            originator_score = text_consistency(pkg1['originator'], pkg2['author'])
            supplier_score = text_consistency(pkg1['supplier'], pkg2['author'])
            version_score = version_consistency(pkg1['versionInfo'], pkg2['version'])
            dL_score = longest_common_substring_consistency_score(pkg1['downloadLocation'], pkg2['home_page'])

            licenseC_score = license_consistency(pkg1['licenseConcluded'], pkg2['license'])
            licenseD_score = license_consistency(pkg1['licenseDeclared'], pkg2['license'])
            if repo1_flag:
                logger.success(f'[SPDX_Repo_info]: {reponame1}')
                repo1_flag = False
            else:
                result = [originator_score, supplier_score, version_score, dL_score, licenseC_score, licenseD_score]
                all_matched_scores['pkg_info'].append(result)

    if len(all_matched_scores['pkg_info']) == 0:
        all_matched_scores['pkg_info'].append([0, 0, 0, 0, 0, 0])
    if len(all_matched_scores['repo_info']) == 0:
        all_matched_scores['repo_info'] = [0, 0, 0, 0, 0, 0, 0, 0]

    all_matched_scores['statistic_info'] += [len(pkgs_keys1), len(pkgs2), len(matched_pkg)]
    logger.success(f'[FinishedSPDX]: {tool1}||{reponame1}')

    return all_matched_scores


def compare_files(file1_path, file2_path, standard, result_path):
    global fileinvalid, TotalMatchedName
    filevalidflag = True

    if file1_path == file2_path:
        logger.debug(f'[SameFile]: {file1_path}')
        filevalidflag = False
        fileinvalid += 1

    if not is_valid_json(file1_path):
        logger.debug(f'[FileNotExistOrInvalid]: {file1_path}')
        filevalidflag = False
        fileinvalid += 1

    if not is_valid_json(file2_path):
        logger.debug(f'[FileNotExistOrInvalid]: {file2_path}')
        filevalidflag = False
        fileinvalid += 1

    if not filevalidflag:
        return None

    if standard == 'spdx':
        all_matched_scores = spdx_consistency(file1_path, file2_path, result_path)
    elif standard == 'cdx':
        all_matched_scores = cdx_consistency(file1_path, file2_path, result_path)
    else:
        logger.error(f'Invalid standard: {standard}')

    return all_matched_scores


def parse_fileinfo(path):
    string = os.path.basename(path)
    filename = string[:string.rfind(".")]
    with open(path, 'r') as file:
        data = json.load(file)
    if '#' not in filename:
        return filename, data
    else:
        _, tool, reponame = filename.split('#')
        return filename, tool, reponame, data


def write_row2csv(filename: str, data: list) -> None:
    with open(filename, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(data)


def run_accuracy_evaluator(standard, bench_path, ori_filepath, result_path, reponames):
    tools_spdx = ['syft', 'gh-sbom', 'ort', 'sbom-tool']
    tools_cdx = ['syft', 'gh-sbom', 'ort', 'scancode', 'cdxgen']
    result_path = os.path.join(result_path, 'accuracy')

    if not os.path.exists(result_path):
        os.makedirs(result_path)

    if standard == 'spdx':
        tools = tools_spdx
    elif standard == 'cdx':
        tools = tools_cdx
    for tool in tools:
        wbfile = os.path.join(result_path, f'{standard}-{tool}-accuracy.csv')
        with open(wbfile, 'w', newline='') as fd:
            writer = csv.writer(fd)
            if standard == 'cdx':
                writer.writerow(['repo_name', 'repo_name_meta', 'repo_version', 'comp_num', 'comp_bench', 'matched_comps',
                                 'author_score', 'license_score', 'version_score'])
            elif standard == 'spdx':
                writer.writerow(['repo_name', 'pkgs_num', 'pkgs_bench', 'matched_pkgs',
                                 # repo
                                 'doc_name', 'originator_score_r', 'supplier_score_r', 'copyright_score_r', 'version_score_r',
                                 'dL_score_r', 'licenseC_score_r', 'licenseD_score_r',
                                 # pkg
                                 'originator_score', 'supplier_score', 'version_score', 'dL_score', 'licenseC_score', 'licenseD_score'])
    bench_list = []
    with open(os.path.join(bench_path, 'bench.list'), 'r') as fd:
        line = fd.readline()
        while line:
            line = line.strip()
            bench_list.append(line)
            line = fd.readline()
    bench_match = []
    with open(reponames, 'r') as fd:
        line = fd.readline()
        while line:
            line = line.strip()
            if f'{line}.json'.lower() not in bench_list:
                line = fd.readline()
                continue
            bench_match.append(f'{line}.json'.lower())
            for tool in tools:
                filepath = os.path.join(ori_filepath, f'{standard}#{tool}#{line}.json')
                benchpathlower = os.path.join(bench_path, f'json-data/{line}.json').lower()
                results = compare_files(filepath, benchpathlower, standard, result_path)
                if results is not None:
                    if standard == 'cdx':
                        row = [line] + results['repo_info'] + results['statistic_info'] + \
                            [sum(x) / len(x) for x in zip(*results['pkg_info'])]
                    elif standard == 'spdx':
                        row = [line] + results['statistic_info'] + results['repo_info'] + \
                            [sum(x) / len(x) for x in zip(*results['pkg_info'])]
                    wbfile = os.path.join(result_path, f'{standard}-{tool}-accuracy.csv')
                    write_row2csv(wbfile, row)
            line = fd.readline()
    logger.info(f'[TotalBenchmark]: {len(bench_match)}')
    for bench in bench_list:
        if bench in bench_match:
            continue
        else:
            pass
            # logger.error(f'[MissedBenchmark]: {bench}')
    return result_path
# spdx
# all_matched['pkg_info' or 'repo_info'] = originator_score, supplier_score, copyright_score, version_score, PVC_score,
#                                          cpe_score, purl_score, dL_score, licenseC_score, licenseD_score
# all_matched['files_info'] = f1['name'], checksum_score
# all_matched['statistic_info'] = [len(files_keys1), len(files_keys2), len(matched_files), len(pkgs_keys1), len(pkgs_keys2), len(matched_pkg)]

# cdx
# all_matched['repo_info'] = [name_com, version_com]
# all_matched['pkg_info'] = [author_score, type_score, purl_score, cpe_score, license_score, version_score]
# all_matched['statistic_info'] = [len(component_keys1), len(component_keys2), len(matched_pkg)]


if __name__ == '__main__':

    path_result = '/mnt/three-lans/parse_results/accuracy-python'
    if not os.path.exists(path_result):
        os.makedirs(path_result)
    logger.add("/mnt/three-lans/parse_sboms-logs/accuracy-python.log")
    ori_filepath = '/mnt/three-lans/parse_results/field_extraction-python/'
    reponames = '/mnt/github-python/github-python-names-sorted.txt'
    # logger.debug('Running in DEBUG mode...')
    run_accuracy_evaluator('cdx', ori_filepath, path_result, reponames)
    run_accuracy_evaluator('spdx', ori_filepath, path_result, reponames)
    logger.success('Accuracy Analysis Finished.\n\n')
    # logger.debug('DEBUG mode running finished.')
