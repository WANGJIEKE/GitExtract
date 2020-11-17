import csv
from pydriller import RepositoryMining
from pathlib import Path
import traceback
import time
import threading
from typing import List
import sys


def run_worker(i: int, repos: List[Path]) -> None:
    idx = 0

    for repo_path in repos:
        commit_count = 0
        start_time = time.time()

        # noinspection PyBroadException
        try:
            repo_mining = RepositoryMining(str(repo_path), only_modifications_with_file_types=['.py'])

            Path(f'./result{i}/').mkdir(exist_ok=True)

            with open(f'./result{i}/{repo_path.name}.stats.csv', 'w', newline='') as csv_file:

                writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(COLUMNS)

                for commit in repo_mining.traverse_commits():
                    commit_count += 1

                    test_file_modifications = [m for m in commit.modifications if 'test' in m.filename]

                    if len(test_file_modifications) == 0:
                        continue

                    for modification in test_file_modifications:
                        writer.writerow((
                            commit.project_name,
                            commit.msg,
                            modification.old_path,
                            modification.new_path,
                            commit.hash
                        ))

        except Exception:
            tb = traceback.format_exc()

            with open(f'error_log{i}.txt', 'a') as fp:
                fp.write(f'repo_path={repo_path}\n')
                fp.write(f'{tb}\n\n')

        finally:
            used_time = time.time() - start_time

            with open(f'progress_log{i}.txt', 'a') as fp:
                fp.write(f'idx={idx} (out of {len(repos)})\n')
                fp.write(f'repo_path={repo_path}\n')
                fp.write(f'commit_count={commit_count}\n')
                fp.write(f'used_time={used_time} seconds\n\n')

            idx += 1


COLUMNS = ['Project Name', 'Message', 'Old Path', 'New Path', 'Hash']

if len(sys.argv) != 2:
    print(f'error: need specify directory\nusage: {sys.argv[0]} repo_directory', file=sys.stderr)

PATH = sys.argv[1]

# 39204 in total
REPOS = sorted(filter(lambda p: p.is_dir(), Path(PATH).iterdir()))
WORKER_COUNT = 20

REPO_PER_WORKER = len(REPOS) // WORKER_COUNT
REMAINDER = len(REPOS) % WORKER_COUNT

REPO_LIST = [REPOS[i: i + REPO_PER_WORKER] for i in range(0, len(REPOS), REPO_PER_WORKER)]

for i in range(REMAINDER):
    REPO_LIST[i].append(REPOS[-i])

THREAD_LIST = [threading.Thread(None, run_worker, f'worker-{i}', (i, REPO_LIST[i])) for i in range(WORKER_COUNT)]

for t in THREAD_LIST:
    t.start()

for t in THREAD_LIST:
    t.join()
