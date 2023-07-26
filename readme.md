# Dataset Construction

Additional code to construct the two datasets for ACMMM'23 paper *DRIN: A Dynamic Relation Interactive Network for Multimodal Entity Linking*. If you are not interested in how we construct our dataset, you can just visit https://github.com/starreeze/drin to download our constructed dataset and get our code. If you find this helpful, please cite our paper.

## introduction

This repo contains scripts for constructing the two dataset, as the original ones do not provide entity images.

The steps taken to construct the dataset:

1. We first download from Wikidata a file containing all entities (~1TB size);

2. Extract qid <-> entity pairs from the huge json file downloaded;

3. Apply fuzzy search to extract candidate entities for the provided mentions;

4. Use Wikidata API to search for top-10 images for each candidate entity;

5. Clean the images: select one best-quality image for each entity.

The following is some notes taken during development. Hope to be helpful to you if you want to construct a similar dataset from scratch.

## Text data extractor

Create qid -> entity name mapping, used by the both 2 follwing tasks.
Read from json files ['entities']['Qxxxx']['sitelinks']['enwiki']['title']

## Candidate Generation

Create mention -> list[qid] mapping

1. edit_distance(mention name, entity name), fuzzy search same with sota
2. min(edit_distance(mention name, name **for** name **in** search_wikidata_alias(entity name)))

## Wiki Spider

### Usage

Prepare input file with qids (a separate line for each qid), and then run

`python spider.py` after specifying params; or

`python spider.py -c` to retry/continue with previous qids where errors occurred.

### Get image from qid

qid->entity name->image label->image
pageid and revid is not consistent between wikidata and wikipedia API

#### qid->entity name

Just use qid -> entity name mapping created before.

#### entity name->image label

wikimedia API. If no image returned, try alias of the entity.

#### image label->image

wikimedia API
