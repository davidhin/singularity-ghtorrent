from glob import glob

import pandas as pd
import singghtorrent as sg
from tqdm import tqdm

pr_path = str(sg.storage_processed_root() / "pr_comments/*")
cm_path = str(sg.storage_processed_root() / "commit_messages/*")

pr_files = glob(pr_path)
cm_files = glob(cm_path)

sql_injection = []
for pr in tqdm(pr_files):
    df_temp = pd.read_parquet(pr)
    matched = df_temp[df_temp.BODY.str.contains("security vulnerability")]
    if len(matched) > 0:
        print(len(matched))
        sql_injection.append(matched)
pd.concat(sql_injection)
