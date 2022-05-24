import glob
import json
from datetime import datetime, timezone

import pandas as pd
import matplotlib.pyplot as plt

from util import logger, put_github_action_env


class Metric(object):
    def _recent_history_data_in_days(
        self,
        key: str,
        days=7,
        timestamp_key: str = "block_timestamp",
        round_to_day=True,
    ) -> pd.DataFrame:
        to_timestamp = (
            (
                datetime.now()
                .replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
                .timestamp()
            )
            if round_to_day
            else datetime.now().timestamp()
        )

        from_timestamp = to_timestamp - days * 24 * 60 * 60
        all_files = self._recent_history_files(key, -1)
        all_files.reverse()

        logger.debug(f"{key} loading [{from_timestamp}, {to_timestamp}]")

        results = []
        for path in all_files:
            with open(path, "r") as f:
                for line in f:
                    obj = json.loads(line)
                    if "error" in obj:
                        continue
                    if obj[timestamp_key] >= to_timestamp:
                        continue
                    if obj[timestamp_key] < from_timestamp:
                        # enumerate in DESC order, so we can break all
                        return pd.DataFrame(results)
                    results.append(obj)
        return pd.DataFrame(results)

    def _recent_history_objects(self, key: str, limit: int):
        results = []
        files = self._recent_history_files(key, limit)
        logger.info(f"{key} loading {len(files)} files")
        for path in files:
            objs = [json.loads(line) for line in open(path, "r").readlines()]
            results.extend([o for o in objs if "error" not in o])
        logger.info(f"{key} loaded {len(results)} objs")
        return results

    @staticmethod
    def _recent_history_files(key: str, limit: int):
        results = sorted(glob.glob(f"./history/{key}_**.jsonl"))
        if limit < 0:
            return results
        return results[-limit:]

    def generate_recent_tx_fig(self, output: str, days=14):
        df = self._recent_history_data_in_days("transactions", days=days)

        df["datetime"] = pd.to_datetime(df["block_timestamp"], unit="s").round("1d")
        df["post"] = df["id"]
        df["unique_post"] = df["original-content-digest"]
        logger.debug(
            f"{len(df)} txs: {df.iloc[0]['block_timestamp']} - {df.iloc[-1]['block_timestamp']}"
        )
        df = df[["datetime", "post", "unique_post", "contributor"]]

        df = df.groupby("datetime").nunique()
        logger.debug(f"{len(df)} grouped txs: \n{df.head()}")
        df.plot(legend=True, figsize=(12, 8))
        plt.savefig(output)
        # plt.show()

    def generate_metrics(self, output: str):
        last_24h_txs = self._recent_history_data_in_days(
            "transactions", 1, round_to_day=False
        )
        if len(last_24h_txs) == 0:
            logger.warn("No posts found")
            return
        last_tx = last_24h_txs.iloc[-1]

        logger.debug(f"Generating metric from {len(last_24h_txs)} history posts")

        metrics = {
            "updated_at": datetime.now(timezone.utc).astimezone().isoformat(),
            "last_block_height": int(last_tx["block_height"]),
            "last_block_time": datetime.fromtimestamp(last_tx["block_timestamp"])
            .astimezone()
            .isoformat(),
        }

        if last_24h := self.last_24h_tx_metric(last_24h_txs):
            metrics["last_24h"] = last_24h

        logger.debug(f"Metrics: {metrics}")
        with open(output, "w") as f:
            f.write(json.dumps(metrics, ensure_ascii=False, indent=2))

        recent_txs_fig = f"dist/recent_mirror.png"
        self.generate_recent_tx_fig(recent_txs_fig)

        put_github_action_env("METRIC_FILES", "\n".join([output, recent_txs_fig]))

    @staticmethod
    def last_24h_tx_metric(df: pd.DataFrame):
        logger.info(f"Generating 24h metric from {len(df)} history txs")
        if len(df) == 0:
            return None

        post_count = len(df)
        user_count = df["contributor"].nunique()
        unique_post = df["original-content-digest"].nunique()

        return {
            "post": post_count,
            "user": user_count,
            "unique_post": unique_post,
        }


if __name__ == "__main__":
    pd.set_option("display.max_columns", None)

    m = Metric()
    # m.generate_recent_tx()
    m.generate_metrics("dist/metrics.json")
