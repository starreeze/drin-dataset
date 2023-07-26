# -*- coding: utf-8 -*-
# @Date    : 2022-10-12 19:07:24
# @Author  : Shangyu.Xing (starreeze@foxmail.com)
"""
Generate top-k candidates from mentions
"""
from __future__ import annotations
import os, json
from typing import Tuple, Dict
from fuzzywuzzy import fuzz
from multiprocessing import Pool
from tqdm import tqdm

num_candidates = 100
num_process = 24
# stdout_interval = 3
search_wiki = False
qid_entity_path = "entities/qid-entity.tsv"  # qid <-> entity mapping
dataset_mention_dir = "mentions"
output_candidate_path = "candidates/candidates.tsv"
output_all_candidate_qids_filepath = "candidates/all-qids.txt"


def load_entities() -> Tuple[list[str], list[str]]:
    """
    load qid <-> entity mapping
    return: list of qids, list of entity names
    """
    # TODO load alias
    qids, entities = [], []
    with open(qid_entity_path, "r") as f:
        for line in f.readlines():
            items = line.strip().split("\t")
            qids.append(items[0])
            entities.append(items[1])
    return qids, entities


def load_mentions() -> list[Tuple[str, str, str]]:
    """
    load mentions from the original dataset (json files)
    return: list(id, mention, answer qid)
    get qid to see the accuracy of candidate generation
    """
    res = []
    for json_file in os.listdir(dataset_mention_dir):
        if json_file.endswith(".json"):
            with open(os.path.join(dataset_mention_dir, json_file)) as f:
                content: Dict = json.load(f)
            for k, v in content.items():
                res.append((k, v["mentions"], v["answer"]))
    return res


def match(mention: str, entities: list[str]) -> list[Tuple[int, int]]:
    """
    for a mention, calculate the similarity score to each entity
    return : list of (index, score)
    """
    scores = [fuzz.partial_ratio(mention, entity) for entity in entities]
    order = sorted(range(len(scores)), key=scores.__getitem__, reverse=True)
    return [(index, scores[index]) for index in order[:num_candidates]]


def match_batch(
    inputs: Tuple[list[Tuple[str, str, str]], list[str], list[str], int]
) -> Tuple[list[list[str]], list[str], int]:
    mentions, entities, qids, process_id = inputs
    top_qids: list[list[str]] = []
    qids_all: list[str] = []
    num_hits = 0
    pbar = tqdm(enumerate(mentions), total=len(mentions), position=process_id - 1)
    for i, (id, mention, answer_qid) in pbar:
        top_entities = match(mention, entities)
        top_qid = []
        for i in top_entities:
            qid = qids[i[0]]
            top_qid.append(qid)
            qids_all.append(qid)
        top_qids.append([id] + top_qid)
        if answer_qid in top_qid:
            num_hits += 1
    return top_qids, qids_all, num_hits


def generate() -> list[str]:
    """
    generate candidate qids matching each mention and store them in file.
    return all candidate qids for the convenient of the spider
    """
    qids, entities = load_entities()
    mentions = load_mentions()
    num_samples = len(mentions)
    num_pre_process = (num_samples + num_process - 1) // num_process
    mentions = [
        mentions[i * num_pre_process : (i + 1) * num_pre_process]
        for i in range(num_process)
    ]
    process_args = list(
        zip(
            mentions,
            [entities] * num_process,
            [qids] * num_process,
            range(1, num_process + 1),
        )
    )
    with Pool(num_process) as pool:
        outputs = pool.map(match_batch, process_args)
    num_hits = 0
    qids_all: list[str] = []
    with open(output_candidate_path, "w") as f:
        for output in outputs:
            for top_qids in output[0]:
                f.write("\t".join(top_qids) + "\n")
            num_hits += output[2]
            qids_all += output[1]
    print("accuracy:", num_hits / num_samples)
    return list(set(qids_all))


def main() -> None:
    with open(output_all_candidate_qids_filepath, "w") as f:
        f.write("\n".join(generate()))


if __name__ == "__main__":
    main()

