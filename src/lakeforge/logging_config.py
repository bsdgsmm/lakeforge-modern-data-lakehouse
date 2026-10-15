import logging

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s - %(message)s"


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT, force=True)
    for noisy in ("py4j", "pyspark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
