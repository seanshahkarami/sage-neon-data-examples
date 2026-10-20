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

Finally, there are various utils like `show-data-product-stats` and `summarize-data-products` which provide a high level description of the data. For example:

```sh
$ $./summarize-data-products
neon/reading.sensor.aepg600m_heated.parquet
┌─────────────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬───┬──────────────────────┬────────┬─────────────────┐
│         column_name         │     column_type      │         min          │         max          │ … │         q75          │ count  │ null_percentage │
│           varchar           │       varchar        │       varchar        │       varchar        │ … │       varchar        │ int64  │  decimal(9,2)   │
├─────────────────────────────┼──────────────────────┼──────────────────────┼──────────────────────┼───┼──────────────────────┼────────┼─────────────────┤
│ source_id                   │ VARCHAR              │ 16765                │ 16765                │ … │ NULL                 │ 112438 │            0.00 │
│ site_id                     │ VARCHAR              │ CPER                 │ CPER                 │ … │ NULL                 │ 112438 │            0.00 │
│ readout_time                │ TIMESTAMP WITH TIME… │ 2026-10-06 19:08:46… │ 2026-10-20 10:05:04… │ … │ 2026-10-17 00:10:22… │ 112438 │            0.00 │
│ internal_temperature        │ DOUBLE               │ -2.0390000343322754  │ 120.0                │ … │ 20.499190491736194   │ 112438 │            0.95 │
│ inlet_temperature           │ DOUBLE               │ -11.475000381469727  │ 30.743999481201172   │ … │ 11.460359902004836   │ 112438 │            0.92 │
│ strain_gauge1_temperature   │ DOUBLE               │ 0.6639999747276306   │ 40.09600067138672    │ … │ 23.59379161053438    │ 112438 │            0.89 │
│ strain_gauge2_temperature   │ DOUBLE               │ -0.2029999941587448  │ 37.34700012207031    │ … │ 22.153981122637568   │ 112438 │            0.89 │
│ strain_gauge3_temperature   │ DOUBLE               │ 0.2980000078678131   │ 37.066001892089844   │ … │ 22.68296334105174    │ 112438 │            0.89 │
│ strain_gauge1_frequency_raw │ DOUBLE               │ 1729.3800048828125   │ 1864.177001953125    │ … │ 1855.9761453266558   │ 112438 │            0.89 │
│ strain_gauge2_frequency_raw │ DOUBLE               │ 1772.333984375       │ 1919.5810546875      │ … │ 1907.6389357696175   │ 112438 │            0.89 │
│ strain_gauge3_frequency_raw │ DOUBLE               │ 1765.35205078125     │ 1918.2879638671875   │ … │ 1915.141098595812    │ 112438 │            0.89 │
│ strain_gauge1_frequency_co… │ DOUBLE               │ 1729.3800048828125   │ 1864.177001953125    │ … │ 1855.9761453266558   │ 112438 │            0.89 │
│ strain_gauge2_frequency_co… │ DOUBLE               │ 1772.333984375       │ 1919.5810546875      │ … │ 1907.6389357696175   │ 112438 │            0.89 │
│ strain_gauge3_frequency_co… │ DOUBLE               │ 1765.35205078125     │ 1918.2879638671875   │ … │ 1915.141098595812    │ 112438 │            0.89 │
│ strain_gauge1_weight        │ DOUBLE               │ 3.4839999675750732   │ 4.078999996185303    │ … │ 4.041690990684592    │ 112438 │            0.89 │
│ strain_gauge2_weight        │ DOUBLE               │ 3.6110000610351562   │ 4.269000053405762    │ … │ 4.213205538827796    │ 112438 │            0.89 │
│ strain_gauge3_weight        │ DOUBLE               │ 3.61299991607666     │ 4.300000190734863    │ … │ 4.285374106323624    │ 112438 │            0.89 │
│ total_gauge_weight          │ DOUBLE               │ 10.715999603271484   │ 12.628000259399414   │ … │ 12.544072735028942   │ 112438 │            0.89 │
│ total_precipitation_depth   │ DOUBLE               │ 0.0                  │ 10.319000244140625   │ … │ 1.3228892767369116   │ 112438 │            0.91 │
│ orifice_heater_flag         │ INTEGER              │ 0                    │ 111                  │ … │ 0                    │ 112438 │            0.92 │
│ precipitation_accumulation… │ DOUBLE               │ 0.0                  │ 1.6970000267028809   │ … │ 0.04201229548125639  │ 112438 │            0.93 │
│ strain_gauge1_stability     │ INTEGER              │ 1                    │ 1                    │ … │ 1                    │ 112438 │            0.07 │
│ strain_gauge2_stability     │ INTEGER              │ 1                    │ 1                    │ … │ 1                    │ 112438 │            0.15 │
│ strain_gauge3_stability     │ INTEGER              │ 1                    │ 1                    │ … │ 1                    │ 112438 │            0.11 │
└─────────────────────────────┴──────────────────────┴──────────────────────┴──────────────────────┴───┴──────────────────────┴────────┴─────────────────┘
...
```