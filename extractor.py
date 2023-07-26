# -*- coding: utf-8 -*-
# @Date    : 2022-10-12 20:23:54
# @Author  : Shangyu.Xing (starreeze@foxmail.com)
"""
Extract qid <-> entity name mapping from the huge json file
"""

import ijson
from tqdm import tqdm

input_entity_filepath = "latest-all.json"
output_filepath = "qid-entity.tsv"


def main():
    print("Reading JSON file...")
    with open(output_filepath, "w") as outputs:
        with open(input_entity_filepath, "rb") as inputs:
            pbar = tqdm(ijson.items(inputs, "item"))
            failure = 0
            for info in pbar:
                qid = info["id"]
                try:
                    entity_name = info["sitelinks"]["enwiki"]["title"]
                except KeyError:
                    failure += 1
                    pbar.set_description(f"failure: {failure}")
                    continue
                outputs.write(f"{qid}\t{entity_name}\n")


if __name__ == "__main__":
    main()
