# -*- coding: utf-8 -*-
# @Date    : 2022-10-10 19:42:44
# @Author  : Shangyu.Xing (starreeze@foxmail.com)
"""
Spider for getting wikipedia images and briefs from wikidata qid
"""

from __future__ import annotations
from functools import reduce
import sys, os, requests, time, json
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool
from pathlib import Path
from math import inf
from tqdm import tqdm
from urllib.parse import quote, unquote
from .candidates import load_entities


# params that can be freely changed
enable_download_image = True
num_thread = 8
checkpoint_interval = 4096
image_download_path = "images"
image_default_width = ""
# in this list file size limit will be ignored
image_fallback_width = ["1024px", "800px", "640px", "320px", ""]
pixel_image_extension_names = ["jpg", "png", "gif", "jpeg", "tif"]
vector_image_extension_names = ["svg"]
max_file_size = 4 * 2**20  # 4MB
allow_unkonwn_file_size = True
failed_qid_file_path = "failed.txt"
output_qid_brief_path = "qid2brief.json"
qid_file_path = "test.txt"
proxy_url = "http://114.212.87.91:7890"
zip_image = True
# zip_store_dir = "/aliyun/wiki_images"
zip_store_dir = "images_zipped"

# better not change these below
loop_callback = lambda batch_idx: os.system(
    f"tar cvf {zip_store_dir}/batch_{batch_idx}.tar {image_download_path} && rm {image_download_path}/*"
)
batch_size = 1
proxy = {"http": proxy_url, "https": proxy_url}
head = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36"
}
wikipedia_url = "https://en.wikipedia.org/wiki/%s"
# params: qid (Q21)
# returns: full wididata page, see samples/england_wikidata.html

entity_name_query_image_label_brief_url = "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=images|extracts&exintro&explaintext&redirects=1&format=json"
# params: list of entity names separated by |
# returns:
"""
{   "continue": {
        "imcontinue": "9316|Circle-information.svg",
        "continue": "||"
    },
    "query": {
        "pages": {
            "9316": {
                "pageid": 9316,
                "ns": 0,
                "title": "England",
                "extracts": "..."
                "images": [
                    {
                        "ns": 6,
                        "title": "File:09 The Queen's Dolour (A Farewell) Henry Purcell Transcribed Ronald Stevenson (1958) Mark Gasser Piano (Live Recording).ogg"
                    }...]}
"""

image_label_query_image_url = "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=imageinfo&iiprop=url&format=json"
# params: list of file labels separated by |
# returns:
"""
{   "batchcomplete": "",
    "query": {
        "pages": {
            "-1": {
                "ns": 6,
                "title": "File:1 christ church hall 2012.jpg",
                "missing": "",
                "known": "",
                "imagerepository": "shared",
                "imageinfo": [{
                    "url": "https://upload.wikimedia.org/wikipedia/commons/4/49/1_christ_church_hall_2012.jpg",
                    "descriptionurl": "https://commons.wikimedia.org/wiki/File:1_christ_church_hall_2012.jpg",
                    "descriptionshorturl": "https://commons.wikimedia.org/w/index.php?curid=18896931"
                }]}}}}
"""


def url_quote(s: str) -> str:
    return quote(s.encode("utf8"))


def url_unquote(s: str) -> str:
    return unquote(s, encoding="utf8")


def assign_resolution(url: str, res: str) -> str:
    if res == "":
        return url
    # find the 5th '/' in url and add '/thumb' before it
    i, count = 0, 0
    while True:
        if url[i] == "/":
            count += 1
            if count == 5:
                break
        i += 1
    url = url[:i] + "/thumb" + url[i:]
    # add suffix
    url += f"/{res}-" + url[url.rfind("/") + 1 :]
    extension = url[url.rfind(".") + 1 :]
    if extension in vector_image_extension_names:
        url += ".png"
    return url


def get(url: str, retries=3, sleeps=1, timeout=6, **kwargs) -> requests.Response:
    while retries:
        try:
            r = requests.get(
                url,
                timeout=timeout,
                headers=head,
                allow_redirects=True,
                proxies=proxy,
                **kwargs,
            )
            break
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.TooManyRedirects,
        ):
            retries -= 1
            time.sleep(sleeps)
    if retries == 0:
        raise requests.exceptions.ConnectionError("All retries failed: {}".format(url))
    return r


def entity_name_query_brief(entity: str) -> str:
    labels_json = get(
        "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=extracts&exintro&explaintext&redirects=1&format=json"
        % url_quote(entity)
    ).json()
    page_info = list(labels_json["query"]["pages"].values())[0]
    return url_unquote(page_info["extract"].strip().replace("\n", " "))


def entity_name_query_image_label_brief(
    entity_names: list[str],
) -> tuple[list[list[str]], list[str]]:
    """
    input a batch: I once thought API would return all the result at once;
    however, it returns only 10 images labels in total. Not feel like changing this back.
    returns: list of image labels for each qid, size: [batch_size, <=10]; and a list of brief introduction for each entity
    """
    image_labels: list[list[str]] = []
    brief = []

    def append_entity_label_brief(entity: str) -> None:
        nonlocal image_labels, brief
        labels_json = get(
            entity_name_query_image_label_brief_url % url_quote(entity)
        ).json()
        page_info = list(labels_json["query"]["pages"].values())[0]
        for image in page_info["images"]:
            image_labels[-1].append(url_unquote(image["title"].strip()))
        brief.append(url_unquote(page_info["extract"].strip().replace("\n", " ")))

    for entity in entity_names:
        image_labels.append([])
        try:
            append_entity_label_brief(entity)
        except KeyError:
            # no images: page redirected, see https://en.wikipedia.org/wiki/China_(region)
            # get full wikipedia page to obtain real entity
            soup = bs(get(wikipedia_url % url_quote(entity)).content, "lxml")
            new_entity = soup.title.text  # 'Greater China - Wikipedia'
            append_entity_label_brief(new_entity[: new_entity.rfind(" -")])
    return image_labels, brief


def image_label_query_image(labels: list[str]) -> list[str]:
    """
    input a batch (in just one query)
    returns: list of image download URLs
    """
    images_json = get(image_label_query_image_url % url_quote("|".join(labels))).json()
    res = []
    for image in images_json["query"]["pages"].values():
        url: str = image["imageinfo"][0]["url"].strip()
        extension_name = url[url.rfind(".") + 1 :]
        # if not an image, reject
        if extension_name in pixel_image_extension_names:
            url = assign_resolution(url, image_default_width)
            res.append(url)
        elif extension_name in vector_image_extension_names:
            res.append(url)
    return res


def download_image(url: str, fileid: str) -> bool:
    def _write_file(content, path):
        with open(path, "wb") as f:
            f.write(content)

    def _download(url: str, fileid: str, file_size_limit: int) -> bool:
        response = get(url, stream=True)
        if response.status_code != 200:
            return False
        if file_size_limit == 0:  # unlimited
            _write_file(
                response.content,
                os.path.join(image_download_path, fileid + url[url.rfind(".") :]),
            )
            return True
        try:
            file_size = int(response.headers["content-length"])
        except KeyError:
            file_size = 0 if allow_unkonwn_file_size else inf
        if file_size < file_size_limit:
            _write_file(
                response.content,
                os.path.join(image_download_path, fileid + url[url.rfind(".") :]),
            )
            return True
        return False

    if _download(url, fileid, max_file_size):
        return True
    for res in image_fallback_width:
        res_url = assign_resolution(url, res)
        if _download(res_url, fileid, 0):
            return True
    return False


def process_batch_qids(
    qids: list[str], qid2entity: dict[str, str]
) -> tuple[list[str], list[str]]:
    # input a batch; please ensure qid is unique
    failed = []
    try:
        entities = [qid2entity[qid] for qid in qids]
        if enable_download_image:
            image_labels, briefs = entity_name_query_image_label_brief(entities)
            image_urls = [image_label_query_image(labels) for labels in image_labels]
            ok = False
            for i, urls in enumerate(image_urls):
                for j, url in enumerate(urls):
                    if download_image(url, f"{qids[i]}-{j}"):
                        ok = True
            if not ok:
                raise Exception(f"image download all failed for {qids}")
        else:
            briefs = [entity_name_query_brief(entity) for entity in entities]
        return briefs, []
    except KeyboardInterrupt:
        exit(1)
    except KeyError:
        return [""] * len(qids), []
    except Exception as e:
        print(e)
        return [""] * len(qids), qids


class QidProcessRes:
    qid2entity = {}
    completed_qids: set[str] = set()

    @staticmethod
    def load_qid_entity_dict():
        map_qids, map_entities = load_entities()
        for k, v in zip(map_qids, map_entities):
            QidProcessRes.qid2entity[k] = v

    @staticmethod
    def get_completed_qids():
        for filename in os.listdir(image_download_path):
            QidProcessRes.completed_qids.add(filename.split("-")[0])


def process_run(inputs: tuple[int, list[list[str]]]) -> tuple[list[str], list[str]]:
    briefs: list[str] = []
    failed: list[str] = []
    for qid in tqdm(inputs[1], total=len(inputs[1]), position=inputs[0]):
        res = process_batch_qids(qid, QidProcessRes.qid2entity)
        briefs += res[0]
        failed += res[1]
    return briefs, failed


def process_all(qids: list[str]) -> tuple[list[str], list[str]]:
    """
    get images and briefs for all qids.
    return : briefs
    """
    qids = list(filter(lambda qid: qid not in QidProcessRes.completed_qids, qids))
    l = len(qids)
    batched_qids = [
        qids[batch_size * i : batch_size * (i + 1)]
        for i in range((l + batch_size - 1) // batch_size)
    ]
    l = len(batched_qids)
    num_pre_thread = (l + num_thread - 1) // num_thread
    process_args = list(
        zip(
            list(range(1, num_thread + 1)),
            [
                batched_qids[i * num_pre_thread : (i + 1) * num_pre_thread]
                for i in range(num_thread)
            ],
        )
    )
    with Pool(num_thread) as pool:
        outputs = pool.map(process_run, process_args)
    return reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), outputs, ([], []))


def main():
    Path(image_download_path).mkdir(exist_ok=True)
    Path(zip_store_dir).mkdir(exist_ok=True)
    QidProcessRes.load_qid_entity_dict()
    QidProcessRes.get_completed_qids()
    with open(qid_file_path, "r") as f:
        qids = [line.strip() for line in f.readlines() if line != "" and line != "\n"]
    batch_size = checkpoint_interval
    num_batches = (len(qids) + batch_size - 1) // batch_size
    if len(sys.argv) > 2:
        global enable_download_image
        enable_download_image = True
        if sys.argv[1] == "-s":  # start with
            start_idx = int(sys.argv[1])
            batch_list = list(range(start_idx, num_batches))
        elif sys.argv[1] == "-l":  # specify full list
            batch_list = [int(idx) for idx in sys.argv[2:]]
        else:
            raise ValueError("params error.")
    else:
        batch_list = list(range(num_batches))
    qid2brief = {}
    fails = []
    for i, batch in tqdm(enumerate(batch_list), total=len(batch_list)):
        print(f"process batch {batch} ({i} / {len(batch_list)}) with size {batch_size}")
        batch_qids = qids[
            batch * checkpoint_interval : (batch + 1) * checkpoint_interval
        ]
        briefs, fail = process_all(batch_qids)
        for qid, brief in zip(batch_qids, briefs):
            if brief:
                qid2brief[qid] = brief
        fails += fail
        if enable_download_image and loop_callback(batch):
            print(f"Error storing file on batch {batch}!")
    with open(output_qid_brief_path, "w") as f:
        json.dump(qid2brief, f)
    with open(failed_qid_file_path, "w") as f:
        f.write('\n'.join(fails))


if __name__ == "__main__":
    main()
