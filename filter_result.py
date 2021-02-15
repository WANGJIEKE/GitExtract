#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
from pandas import errors as pd_err


CLEANED_RESULT_ROOT_DIR = Path('./cleaned_result')
RESULT_ROOT_DIR = Path('.')


def is_py(old_path, new_path):
    return Path(old_path).suffix == '.py' or Path(new_path).suffix == '.py'


if __name__ == '__main__':
    for result_dir in filter(lambda d: d.is_dir(), RESULT_ROOT_DIR.iterdir()):
        for csv_file_path in filter(lambda f: f.suffix == '.csv', result_dir.iterdir()):
            try:
                df = pd.read_csv(csv_file_path)
            except pd_err.EmptyDataError:
                continue
            df.fillna('', inplace=True)

            if len(df) == 0:
                continue

            df['is_py'] = df.apply(lambda x: is_py(x['Old Path'], x['New Path']), axis=1)
            df: pd.DataFrame = df[df['is_py']].copy().drop(columns=['is_py'])  

            if len(df) == 0:
                continue

            (CLEANED_RESULT_ROOT_DIR / csv_file_path.parent.name).mkdir(exist_ok=True)
            df.to_csv(CLEANED_RESULT_ROOT_DIR / csv_file_path.parent.name / csv_file_path.name)
