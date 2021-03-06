import asyncio
import itertools
import math
import json
import os
import time
from typing import Union

from metric import Metric
from util import logger, read_last_jsonline, chunks
from arweave import ArweaveFetcher

GITHUB_FILE_LIMIT = 100 * 1024 * 1024  # 100 MB


class Tracker(object):
    history_folder = "history"

    transactions_path = "transactions.jsonl"
    posts_path = "posts.jsonl"

    def __init__(
        self,
        tags: list[dict[str, Union[str, list[str]]]],
        transformer,
        history_batch_size: int = 1000,
    ):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)
        self.history_batch_size = history_batch_size
        os.makedirs(self.history_folder, exist_ok=True)
        self.cursor = None
        self.batch_size = 100
        self.last_tx = read_last_jsonline(self.transactions_path)

    def start_tracking(
        self,
        keep_tracking: bool = False,
        keep_recent_count: int = None,
        generate_feed: bool = True,
    ):
        start_time = time.time()
        logger.info(f"Starting tracking keep_tracking: {keep_tracking}")
        while self._run_once():
            if not keep_tracking:
                break
            if time.time() - start_time >= 1200:
                # commit every 20 min
                break

        if keep_recent_count:
            self.truncate(line_count=keep_recent_count)

        if generate_feed:
            self.generate_feed()

        # allow metrics fail
        try:
            m = Metric()
            m.generate_metrics("dist/metrics.json")
        except Exception as e:
            logger.error(f"Failed to generate metric: {e}")

        self.split_large_history_files_if_needed()

    def _run_once(self):
        limit = self.batch_size

        min_block = self.last_tx["block_height"] if self.last_tx else None

        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.cursor, min_block=min_block, limit=limit
        )

        logger.info(
            f"Fetched {len(txs)} transactions, has_next: {has_next}, cursor: {cursor}, last_tx: {self.last_tx}"
        )
        if len(txs) == 0:
            return False

        # trim duplicated txs when
        # no cursor -> fetch by block height, which will have duplicated txs
        if self.last_tx is not None and self.cursor is None:
            txs = list(
                itertools.dropwhile(lambda t: t["id"] != self.last_tx["id"], txs)
            )
            if len(txs) <= 1:
                logger.info(f"No new transactions, cursor: {cursor}")
                # all txs are duplicated, try again with new cursor
                self.cursor = cursor
                return True
            txs = txs[1:]

        group_by_keys_txs = {}
        for tx in txs:
            key = (
                tx["block_height"] // self.history_batch_size * self.history_batch_size
            )
            group_by_keys_txs.setdefault(key, []).append(tx)

        group_by_keys_posts = {}
        for key, txs in group_by_keys_txs.items():
            ids = [tx["id"] for tx in txs]
            posts = asyncio.run(self.fetcher.batch_fetch_data(ids))
            logger.info(f"{key} Fetched {len(posts)} posts")
            group_by_keys_posts[key] = posts

        # save after success
        self.cursor = cursor
        for key, txs in group_by_keys_txs.items():
            self.append_to_file(key, self.transactions_path, txs)
        for key, posts in group_by_keys_posts.items():
            self.append_to_file(key, self.posts_path, posts)

        return has_next

    # make sure no files larger than 100MB(GitHub limit)
    def split_large_history_files_if_needed(self):
        for p in os.listdir(self.history_folder):
            path = os.path.join(self.history_folder, p)
            size = os.path.getsize(path)
            if size >= GITHUB_FILE_LIMIT:
                # NOTE: this is a hack to split large history files
                # x2 to make sure we split to smaller files
                parts = math.ceil(size / GITHUB_FILE_LIMIT) * 2
                self._split_file(path, parts)

    @staticmethod
    def _split_file(path: str, chunk_count: int):
        logger.info(f"Splitting file {path} to {chunk_count} parts")
        with open(path, "r") as f:
            total_lines = f.readlines()
        chunk_size = math.ceil(len(total_lines) / chunk_count)
        comps = path.split(".")
        offset = 1
        while os.path.exists(".".join(comps[:-1]) + f".{offset}.json"):
            offset += 1
        for i, lines in enumerate(chunks(total_lines, chunk_size)):
            new_path = ".".join(comps[:-1]) + f".{i + offset}.json"
            with open(new_path, "a") as f:  # append to file
                f.writelines(lines)
        os.remove(path)

    def truncate(self, interval: int = None, line_count: int = None):
        self._truncate(self.transactions_path, "block_timestamp", interval, line_count)
        self._truncate(self.posts_path, "timestamp", interval, line_count)

    @staticmethod
    def _truncate(path: str, timestamp_key: str, interval: int, line_count: int):
        if interval is None and line_count is None:
            return

        logger.info(
            f"Truncating {path} with interval: {interval}, line_count: {line_count}"
        )

        with open(path, "r") as f:
            lines = f.readlines()

        if line_count is not None:
            if len(lines) <= line_count and interval is None:
                return
            lines = lines[-line_count:]

        # NOTE: truncate by time will cause transactions not match posts since they have different timestamp
        start_time = time.time() - interval if interval else None
        with open(path, "w") as f:
            for line in lines:
                if start_time is not None:
                    obj = json.loads(line)
                    if obj[timestamp_key] < start_time:
                        continue
                    # post is not ordered in same block, so we simply check every post
                    # start_time = None
                f.write(line)

    def generate_feed(self):
        from feed import generate_all_feeds

        with open(self.posts_path, "r") as f:
            posts = [json.loads(line) for line in f.readlines()]
            posts = list(filter(lambda p: "error" not in p, posts))
            generate_all_feeds(posts)

    # json lines
    # append to current files and history files
    def append_to_file(self, key: int, path: str, dicts: list[dict]):
        logger.info(f"{key} Appending {len(dicts)} to {path}")

        parts = path.split(".")
        name = ".".join(parts[:-1])
        ext = parts[-1]

        with open(path, "a") as f, open(
            os.path.join(self.history_folder, f"{name}_{key:09d}.{ext}"), "a"
        ) as hf:
            for d in dicts:
                s = json.dumps(d, ensure_ascii=False)
                hf.write(s + "\n")

                # truncate body if needed
                if "body" in d:
                    body = d["body"].split("\n")
                    if len(body) > 800:
                        logger.info(
                            f"Truncated body of id: {d['id']}, title: {d['title']} from {len(body)}"
                        )
                        d["body"] = "\n".join(body[:400])
                        s = json.dumps(d, ensure_ascii=False)

                f.write(s + "\n")
