"""
Generate top-k candidates in all wiki entities from mentions
"""
from __future__ import annotations
import os, json
from fuzzywuzzy import process
from multiprocessing import Pool
from tqdm import tqdm

num_candidates = 100
num_process = 32
entity2qid_path = "candidates/ne2qid.json"
mention_path = "mentions/WIKIMEL_%s.json"
output_path = "candidates/top100/candidates-answer.tsv"


def run(inputs) -> dict[str, list[str]]:
    (
        entity2qid,
        candidates,
        mentions,
        pid
    ) = inputs
    res = {}
    for id, mention in tqdm(mentions, position=pid):
        extracted = process.extract(mention, candidates, limit=num_candidates)
        res[id] = [entity2qid[e[0]] for e in extracted]
    return res


def main():
    with open(entity2qid_path, "r") as f:
        entity2qid = json.load(f)
    candidates = list(entity2qid.keys())
    id2mention = {}
    for type in ["train", "valid", "test"]:
        with open(mention_path % type, "r") as f:
            mentions = json.load(f)
            for id, info in mentions.items():
                id2mention[id] = info["mentions"]
    mentions = list(id2mention.items())
    num_samples = len(mentions)
    num_pre_process = (num_samples + num_process - 1) // num_process
    mentions = [
        mentions[i * num_pre_process : (i + 1) * num_pre_process]
        for i in range(num_process)
    ]
    res = Pool(num_process).map(
        run,
        ((entity2qid, candidates, mention, i) for i, mention in enumerate(mentions)),
    )
    # res = [run((entity2qid, candidates, mention, i)) for i, mention in enumerate(mentions)]
    id2candidate = {}
    for r in res:
        id2candidate.update(r)
    with open(output_path, "w") as f:
        for id, candidates in id2candidate.items():
            f.write("\t".join([id] + candidates) + "\n")


if __name__ == "__main__":
    main()

