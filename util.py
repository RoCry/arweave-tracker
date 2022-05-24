import asyncio
import json
import logging
import os
from typing import Union, Any, Optional

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s - [%(levelname)s] > %(message)s",
)
logger = logging.getLogger("arweave-tracker")
logger.level = logging.getLevelName(
    os.environ.get("LOGLEVEL", logging.getLevelName(logging.INFO)).upper()
)


async def get(session: aiohttp.ClientSession, url: str, timeout: int = 10):
    async with session.get(url, raise_for_status=True, timeout=timeout) as resp:
        return await resp.json()


async def batch_get(
    urls: list[str],
    timeout: int = 10,
    return_exceptions=False,
) -> tuple[Union[BaseException, Any], ...]:
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(asyncio.ensure_future(get(session, url, timeout=timeout)))
        results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
        return results


def read_last_line(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    with open(path, "rb") as file:
        try:
            file.seek(-2, os.SEEK_END)
            while file.read(1) != b"\n":
                file.seek(-2, os.SEEK_CUR)
        except OSError:
            file.seek(0)
        return file.readline().decode()


def read_last_jsonline(path: str) -> Optional[dict]:
    line = read_last_line(path)
    if line is None:
        return None
    return json.loads(line)


def test_read_last_line():
    assert read_last_line("tests/not_exists_file") is None

    assert read_last_line("tests/line1.txt").strip() == '{"foo": "bar"}'
    assert json.loads(read_last_line("tests/line1.txt"))["foo"] == "bar"
    assert read_last_line("tests/line2.txt").strip() == '{"foo": "bar"}'
    assert json.loads(read_last_line("tests/line2.txt"))["foo"] == "bar"


def lines_of_file(path: str) -> int:
    with open(path, "r") as f:
        return len(f.readlines())


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def put_github_action_env(key: str, value: str):
    env_file = os.getenv("GITHUB_ENV")
    if env_file is None:
        raise Exception(f"GITHUB_ENV is not set, cannot set {key}={value}")

    with open(env_file, "a") as f:
        f.write(f"{key}<<EOF\n{value}\nEOF\n")
