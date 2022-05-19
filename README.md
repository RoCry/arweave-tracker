# Metrics

### Arweave

![](https://img.shields.io/badge/dynamic/json?label=Height&query=%24.height&url=https%3A%2F%2Fwww.arweave.net?cacheSeconds=60)
![](https://img.shields.io/badge/dynamic/json?label=Blocks&query=%24.blocks&url=https%3A%2F%2Fwww.arweave.net?cacheSeconds=60)
![](https://img.shields.io/badge/dynamic/json?label=Peers&query=%24.peers&url=https%3A%2F%2Fwww.arweave.net?cacheSeconds=60)
![](https://img.shields.io/badge/dynamic/json?label=Queue&query=%24.queue_length&url=https%3A%2F%2Fwww.arweave.net?cacheSeconds=60)
![](https://img.shields.io/badge/dynamic/json?label=Latency&query=%24.node_state_latency&url=https%3A%2F%2Fwww.arweave.net?cacheSeconds=60)

### Tracker

![](https://img.shields.io/badge/dynamic/json?label=UpdatedTime&query=%24.updated_at&url=https%3A%2F%2Fgithub.com%2FRoCry%2Farweave-tracker%2Freleases%2Fdownload%2Flatest%2Fmetrics.json)
![](https://img.shields.io/badge/dynamic/json?label=BlockHeight&query=%24.last_block_height&url=https%3A%2F%2Fgithub.com%2FRoCry%2Farweave-tracker%2Freleases%2Fdownload%2Flatest%2Fmetrics.json)
![](https://img.shields.io/badge/dynamic/json?label=BlockTime&query=%24.last_block_time&url=https%3A%2F%2Fgithub.com%2FRoCry%2Farweave-tracker%2Freleases%2Fdownload%2Flatest%2Fmetrics.json)

### Mirror in 24h

![](https://img.shields.io/badge/dynamic/json?label=Post&query=%24.last_24h.post&url=https%3A%2F%2Fgithub.com%2FRoCry%2Farweave-tracker%2Freleases%2Fdownload%2Flatest%2Fmetrics.json)
![](https://img.shields.io/badge/dynamic/json?label=User&query=%24.last_24h.user&url=https%3A%2F%2Fgithub.com%2FRoCry%2Farweave-tracker%2Freleases%2Fdownload%2Flatest%2Fmetrics.json)
![](https://img.shields.io/badge/dynamic/json?label=Unique%20Post&query=%24.last_24h.unique_post&url=https%3A%2F%2Fgithub.com%2FRoCry%2Farweave-tracker%2Freleases%2Fdownload%2Flatest%2Fmetrics.json)

# Introduction

Track the data on arweave with GitHub Action and store it as [JSON Lines](https://jsonlines.org/)

Check [transactions.jsonl](https://github.com/RoCry/arweave-tracker/blob/deploy/transactions.jsonl) or [posts.jsonl](https://github.com/RoCry/arweave-tracker/blob/deploy/posts.jsonl) as example.


## Tips

If you want to track other data on arweave, you can change the tag filters and some transform code.

# Pending Features

No pending features.
