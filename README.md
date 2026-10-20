# Sage / NEON Data Examples

This repo has some of my personal data examples using NEON data within Sage.

## Setup

A lot of the tools are built on [kcat](https://github.com/edenhill/kcat)... the only problem is, the typical builds I've found do not come with Avro decoding included...

To address this, I've provided a Dockerfile which builds kcat with Avro included. You can build it with:

```sh
podman build -t kcat-avro kcat-avro/
```

Once that's built, I have a few tools to export data.

* `list-topics`: List the topics from the broker.
* `preview-topic`: Show the latest 10 items for a topic.
* `read-topc`: Read the entire topic from the beginning.

I also have a tool which converts some example topics into Parquet tables by running:

```sh
./export-data
./create-data-products
```

This gives you tabular versions of some example sensors readings.

```
memory D select * from 'neon/reading.sensor.hmp155.parquet' limit 10;
┌───────────┬─────────┬────────────────────────────┬────────────────────┬────────────────────┬────────────────────┬─────────────┐
│ source_id │ site_id │        readout_time        │ relative_humidity  │    temperature     │     dew_point      │ error_state │
│  varchar  │ varchar │  timestamp with time zone  │       double       │       double       │       double       │   boolean   │
├───────────┼─────────┼────────────────────────────┼────────────────────┼────────────────────┼────────────────────┼─────────────┤
│ 52608     │ CPER    │ 2026-10-06 09:55:16.769-05 │ 23.753999710083008 │ 22.569000244140625 │  0.796999990940094 │ false       │
│ 55858     │ CPER    │ 2026-10-06 09:55:17.201-05 │  23.81399917602539 │   22.7810001373291 │ 1.0110000371932983 │ false       │
│ 52608     │ CPER    │ 2026-10-06 09:55:17.623-05 │  23.75200080871582 │  22.56999969482422 │  0.796999990940094 │ false       │
│ 55858     │ CPER    │ 2026-10-06 09:55:18.519-05 │ 23.827999114990234 │ 22.781999588012695 │ 1.0210000276565552 │ false       │
│ 52608     │ CPER    │ 2026-10-06 09:55:18.534-05 │  23.75200080871582 │  22.56999969482422 │  0.796999990940094 │ false       │
│ 52608     │ CPER    │ 2026-10-06 09:55:19.405-05 │  23.74799919128418 │  22.57200050354004 │  0.796999990940094 │ false       │
│ 55858     │ CPER    │ 2026-10-06 09:55:19.46-05  │ 23.827999114990234 │ 22.781999588012695 │ 1.0210000276565552 │ false       │
│ 55858     │ CPER    │ 2026-10-06 09:55:20.309-05 │ 23.836999893188477 │ 22.784000396728516 │ 1.0269999504089355 │ false       │
│ 52608     │ CPER    │ 2026-10-06 09:55:20.987-05 │ 23.736000061035156 │ 22.572999954223633 │ 0.7910000085830688 │ false       │
│ 55858     │ CPER    │ 2026-10-06 09:55:21.166-05 │ 23.836999893188477 │ 22.784000396728516 │ 1.0269999504089355 │ false       │
└───────────┴─────────┴────────────────────────────┴────────────────────┴────────────────────┴────────────────────┴─────────────┘
  10 rows                                                                                                             7 columns
```