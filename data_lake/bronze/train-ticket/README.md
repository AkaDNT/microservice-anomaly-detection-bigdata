# Bronze Train-Ticket

For local development, raw Train-Ticket files remain in:

```text
data/raw/train-ticket/
```

This folder is reserved for a copied or HDFS-mounted bronze landing zone if the project later moves from local files to a fuller data lake setup.

The current pipeline should read raw data from `data/raw/train-ticket` and write normalized outputs to `data_lake/silver`.
