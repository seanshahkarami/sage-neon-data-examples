# Sage / NEON Data Examples

This repo has some of my personal data examples using NEON data within Sage.

## Setup

A lot of the tools are built on [kcat](https://github.com/edenhill/kcat)... the only problem is, the typical builds I've found do not come with Avro decoding included...

To address this, I've provided a Dockerfile which builds kcat with Avro included. You can build it with:

```
podman build -t kcat-avro kcat-avro/
```
