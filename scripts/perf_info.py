#!/usr/bin/env python3

import json
import logging
import os

logger = logging.getLogger()


def main(filename: str) -> None:
    with open(filename) as fd:
        s = fd.read()
    js = json.loads(s)
    fname = js["max_footprint_fname"]
    lineno = js["max_footprint_lineno"]
    alloc_samples = js["alloc_samples"]
    if fname.startswith(os.getcwd()):
        fname = fname[1 + (len(os.getcwd())) :]
    logger.info(f"alloc_samples: {alloc_samples}")
    logger.info(f"max_footprint_fname: {fname}:{lineno}")
    logger.info(f"max_footprint_mb: {js['max_footprint_mb']:.0f}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main("scalene-profile.json")
