import os
import sys
import json
import subprocess
from tqdm import tqdm
from pathlib import Path
import concurrent.futures
from loguru import logger

# NOTE: This scipt is used to clone the repositories from GitHub to the local machine. Which is needed for the generation process of new SBOMs.
# Repositories cloned by this script are not compatible with the SAP codebase due to the different file structure.
# But it can be easily adapted to the SAP codebase by modifying the input directory in each script from the generation process of new SBOMs.

# If you want to reproduce the evaluation results of the paper, you can download the generated SBOMs from Zenodo(https://zenodo.org/records/14998624) and unzip them into the test-sbom-files directory.
# Then you can run the following command to reproduce the evaluation results:
# python test-run.py  # you need to modify the corresponding input directory and input metadata file path as in the "metadata-files" directory.
# The results will be saved in the test-sbom-results directory.


class RepoCloner:
    def __init__(self, output_dir="./repos", max_workers=5, timeout=300, proxy=None):
        self.output_dir = Path(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        logger.add(self.output_dir / "clone.log")
        logger.info(f"Output directory set to: {self.output_dir.resolve()}")  # Absolute path logging
        self.max_workers = max_workers
        self.timeout = timeout
        self.proxy = proxy
        self.cloned_repos = set()
        self.failed_repos = {}
        self.state_file = self.output_dir / ".clone_state.json"
        self.stop_due_to_disk = False

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Load previous state if exists
        self._load_state()

    def _load_state(self):
        """Load previous cloning state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.cloned_repos = set(state.get('cloned_repos', []))
                    self.failed_repos = state.get('failed_repos', {})
                    logger.info(f"Loaded state: {len(self.cloned_repos)} cloned, {len(self.failed_repos)} failed")
            except Exception as e:
                logger.error(f"Failed to load state: {e}")

    def _save_state(self):
        """Save current cloning state to file"""
        try:
            state = {
                'cloned_repos': list(self.cloned_repos),
                'failed_repos': self.failed_repos
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _check_disk_space(self, path="/data", min_gb=50):
        """Check free disk space in GB for the given path"""
        stat = os.statvfs(path)
        free_bytes = stat.f_bavail * stat.f_frsize
        free_gb = free_bytes / (1024 ** 3)
        return free_gb

    def clone_repo(self, repo_info):
        """Clone a single repository and checkout to specific commit"""
        # Check disk space
        free_gb = self._check_disk_space(self.output_dir, 50)
        if free_gb < 50:
            logger.warning(f"Insufficient disk space: {self.output_dir} has {free_gb:.2f}GB remaining, below 50GB threshold. Will stop after current repo.")
            self.stop_due_to_disk = True

        try:
            # Extract repo_url and commit_id from repo_info
            if isinstance(repo_info, dict):
                repo_url = repo_info.get('repo_url')
                commit_id = repo_info.get('commit-id')
            else:
                # Backward compatibility: if repo_info is just a string URL
                repo_url = repo_info
                commit_id = None

            # Extract user and repo name
            parts = repo_url.strip().rstrip('/').split('/')
            if len(parts) < 2:
                raise ValueError(f"Invalid repository URL format: {repo_url}")

            repo_name = parts[-1]
            user_name = parts[-2]
            if repo_name.endswith('.git'):
                repo_name = repo_name[:-4]
            # Create directory name in format "user#repo"
            target_dir = self.output_dir / f"{user_name}#{repo_name}"

            # Skip if already cloned
            if repo_url in self.cloned_repos:
                logger.info(f"Skipping already cloned: {repo_url}")
                return True

            # Delete directory if it exists but is in failed state
            if target_dir.exists() and repo_url in self.failed_repos:
                logger.info(f"Cleaning failed clone attempt for: {repo_url}")
                subprocess.run(['rm', '-rf', str(target_dir)], check=True)

            # Clone the repository
            logger.info(f"Cloning {repo_url} to {target_dir}")

            result = subprocess.run(
                ['git', 'clone', f"{self.proxy}/{repo_url}" if self.proxy else repo_url, str(target_dir)],
                capture_output=True,
                text=True,
                timeout=self.timeout
            )

            if result.returncode != 0:
                logger.error(f"Failed to clone {repo_url}: {result.stderr}")
                self.failed_repos[repo_url] = result.stderr
                return False

            # Checkout to specific commit if provided
            if commit_id:
                logger.info(f"Checking out to commit {commit_id} for {repo_url}")
                checkout_result = subprocess.run(
                    ['git', '-C', str(target_dir), 'checkout', commit_id],
                    capture_output=True,
                    text=True,
                    timeout=60
                )

                if checkout_result.returncode != 0:
                    logger.error(f"Failed to checkout {commit_id} for {repo_url}: {checkout_result.stderr}")
                    self.failed_repos[repo_url] = f"Checkout failed: {checkout_result.stderr}"
                    return False

            # Add to cloned repos and remove from failed if it was there
            self.cloned_repos.add(repo_url)
            if repo_url in self.failed_repos:
                del self.failed_repos[repo_url]

            return True

        except subprocess.TimeoutExpired:
            logger.error(f"Timeout while cloning {repo_url}")
            self.failed_repos[repo_url] = "Timeout"
            return False
        except Exception as e:
            logger.error(f"Error cloning {repo_url}: {str(e)}")
            self.failed_repos[repo_url] = str(e)
            return False

    def clone_repos(self, repos_list):
        """Clone multiple repositories in parallel"""
        total_repos = len(repos_list)
        logger.info(f"Starting to clone {total_repos} repositories with {self.max_workers} workers")

        # Filter out already cloned repos
        to_clone = []
        for repo_info in repos_list:
            repo_url = repo_info.get('repo_url') if isinstance(repo_info, dict) else repo_info
            if repo_url not in self.cloned_repos:
                to_clone.append(repo_info)

        logger.info(f"{len(to_clone)} repositories to clone ({len(repos_list) - len(to_clone)} already cloned)")

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_repo = {executor.submit(self.clone_repo, repo): repo for repo in to_clone}

                # Process results with progress bar
                with tqdm(total=len(to_clone), desc="Cloning repositories") as pbar:
                    for future in concurrent.futures.as_completed(future_to_repo):
                        repo_info = future_to_repo[future]
                        repo_url = repo_info.get('repo_url') if isinstance(repo_info, dict) else repo_info
                        try:
                            success = future.result()
                            if success:
                                logger.info(f"Successfully cloned: {repo_url}")
                            else:
                                logger.warning(f"Failed to clone: {repo_url}")
                        except Exception as e:
                            logger.error(f"Exception while cloning {repo_url}: {e}")
                            self.failed_repos[repo_url] = str(e)

                        # Save state periodically
                        self._save_state()
                        pbar.update(1)
                        # Check if need to stop early due to insufficient disk space
                        if self.stop_due_to_disk:
                            logger.error("Insufficient disk space detected, stopped subsequent clone tasks.")
                            break

        except KeyboardInterrupt:
            logger.warning("Interrupted by user, saving current state...")
            self._save_state()
            sys.exit(1)

        # Final save
        self._save_state()

        # Summary
        logger.info(f"Cloning completed. Successfully cloned: {len(self.cloned_repos)}/{total_repos}")
        if self.failed_repos:
            logger.warning(f"Failed to clone {len(self.failed_repos)} repositories")

        # Return statistics
        return {
            'total': total_repos,
            'cloned': len(self.cloned_repos),
            'failed': len(self.failed_repos)
        }

    def retry_failed(self):
        """Retry previously failed repositories"""
        if not self.failed_repos:
            logger.info("No failed repositories to retry")
            return {'total': 0, 'cloned': 0, 'failed': 0}

        failed_repos = list(self.failed_repos.keys())
        logger.info(f"Retrying {len(failed_repos)} failed repositories")
        return self.clone_repos(failed_repos)


def main():
    # Parameters are directly configured here
    input_json = "dataset_repos_commit_info.json"
    output_dir = "cloned_repos"
    max_workers = 16
    timeout = 300
    retry_failed = False
    proxy = "https://gh.hlmg.tech"  # cloudflare workers speed up

    # Read repository list
    try:
        with open(input_json, 'r') as f:
            data = json.load(f)
            # Extract all repositories from all language categories
            repos = []
            for language, repo_dict in data.items():
                for repo_url, repo_info in repo_dict.items():
                    repos.append(repo_info)
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        sys.exit(1)
    logger.info(f"Total repositories to clone: {len(repos)}")

    cloner = RepoCloner(
        output_dir=output_dir,
        max_workers=max_workers,
        timeout=timeout,
        proxy=proxy
    )

    # Retry failed repositories (if needed)
    if retry_failed:
        stats = cloner.retry_failed()
        logger.info(f"Retry completed: {stats['cloned']} succeeded, {stats['failed']} failed")
        return
    logger.info(f"{len(repos)} repositories to clone.")
    unique_repos = set()
    for r in repos:
        repo_url = r.get('repo_url') if isinstance(r, dict) else r
        unique_repos.add(repo_url)
    logger.info(f"{len(unique_repos)} unique repositories identified.")
    # Start cloning
    # stats = cloner.clone_repos(repos[:10])
    # logger.info(f"Cloning completed: {stats['cloned']} succeeded, {stats['failed']} failed out of {stats['total']}")


if __name__ == "__main__":
    main()
