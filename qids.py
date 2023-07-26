# -*- coding: utf-8 -*-
# @Date    : 2022-10-26 08:33:18
# @Author  : Shangyu.Xing (starreeze@foxmail.com)
"""
Qid operations: generate qids from candidates, or obtaining the difference between two qid files
"""

from __future__ import annotations
from tqdm import tqdm
from typing import Iterable

def read_qids(file_path: str) -> list[str]:
    with open(file_path, "r") as f:
        qids = [line.strip() for line in f.readlines() if line != "" and line != "\n"]
    return qids

def write_qids(file_path: str, qids: Iterable[str]) -> None:
    with open(file_path, "w") as f:
        f.write("\n".join(qids))


def diff():
    qids_1 = "candidates/top100/all-qids.txt"
    qids_2 = "candidates/top50/all-qids.txt"
    qids_output = "candidates/top100/qids-100diff50.txt"
    qids_set = set(read_qids(qids_2))
    qids_result = [qid for qid in read_qids(qids_1) if qid not in qids_set]
    write_qids(qids_output, qids_result)


def gen():
    candidate_file = 'candidates/top100/candidates.tsv'
    output_file = 'candidates/top100/qids.txt'
    qids: set[str] = set()
    with open(candidate_file, "r") as f:
        for line in tqdm(f.readlines()):
            for qid in line.strip().split('\t')[1:]:
                qids.add(qid)
    write_qids(output_file, qids)


if __name__ == "__main__":
    gen()
