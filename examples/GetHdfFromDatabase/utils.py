import os
from os.path import isfile
import re
import glob


def check_data_dir(data_dir, glob_str):
    last_idx = None
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    else:
        files = [f for f in glob.glob(data_dir + '/' + glob_str) if isfile(f)]

        idxs = []
        for file in files:
            # find first digit in file name
            s = os.path.basename(file).split('.')[0]
            if (match := re.search(r"\d", s)):
                idxs.append(int(s[match.start():]))

        if len(idxs) != 0:
            idxs.sort()
            last_idx = idxs[-1]
    return last_idx
