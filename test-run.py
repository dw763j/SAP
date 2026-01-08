import os
from loguru import logger
from sap.extractor import run_extract

from sap.compliance_evaluator import run_compliance_evaluator
from sap.consistency_evaluator import run_consistency_evaluator
from sap.accuracy_evaluator import run_accuracy_evaluator

from sap.compliance_analyzer import run_compliance_analyzer
from sap.consistency_analyzer import run_consistency_analyzer
from sap.accuracy_analyzer import run_accuracy_analyzer


def get_project_root():
    current_file = os.path.abspath(__file__)
    repo_collector_dir = os.path.dirname(current_file)
    return repo_collector_dir


if __name__ == '__main__':
    project_root = get_project_root()
    print(f"Project root: {project_root}")
    logger.add(f"{project_root}/test-run.log")
    test_sbom_files = os.path.join(project_root, 'test-sbom-files')
    test_sbom_output = os.path.join(project_root, 'test-sbom-results')
    test_sbom_reponames = os.path.join(project_root, 'test-sbom-reponames.txt')
    accuracy_bench = os.path.join(project_root, 'benchmark-python')
    test_sbom_final_results = os.path.join(test_sbom_output, 'test-sbom-final-results')

    if not os.path.exists(test_sbom_output):
        os.makedirs(test_sbom_output)
    else:
        pass
        # clean up the test output directory if in need
        # for root, dirs, files in os.walk(test_sbom_output):
        #     for file in files:
        #         os.remove(os.path.join(root, file))
    os.makedirs(test_sbom_final_results, exist_ok=True)

    extracted_filelist, extracted_dir = run_extract(root_sboms_path=test_sbom_files, wb_path=test_sbom_output, lans=['python'])

    compliance_dir = run_compliance_evaluator(extracted_dir, extracted_filelist, test_sbom_output)

    run_consistency_evaluator('cdx', test_sbom_output, extracted_dir, test_sbom_reponames, match_mode='triple', triple_score_threshold=2)
    consistency_dir = run_consistency_evaluator('spdx', test_sbom_output, extracted_dir, test_sbom_reponames)

    run_accuracy_evaluator('cdx', accuracy_bench, extracted_dir, test_sbom_output, test_sbom_reponames)
    accuracy_dir = run_accuracy_evaluator('spdx', accuracy_bench, extracted_dir, test_sbom_output, test_sbom_reponames)

    run_compliance_analyzer(compliance_dir, test_sbom_final_results)
    run_consistency_analyzer(consistency_dir, test_sbom_final_results)
    run_accuracy_analyzer(accuracy_dir, test_sbom_final_results)

    print(f"\n\n ============================================\n\nSAP analysis for test-sboms done, all the results are located at {test_sbom_final_results}.")

    print(f"Extracted filelist: {extracted_filelist}")
    print(f"Extracted directory: {extracted_dir}")
    print(f"Compliance directory: {compliance_dir}")
    print(f"Consistency directory: {consistency_dir}")
    print(f"Accuracy directory: {accuracy_dir}")
