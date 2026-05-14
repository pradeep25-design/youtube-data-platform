import os
import json
import logging
from datetime import datetime, timezone


def get_logger(name, log_file=None):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt     = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def get_pipeline_logger(pipeline_name):
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = f"data/logs/{pipeline_name}_{today}.log"
    return get_logger(pipeline_name, log_file)


# -------------------------------------------------------
# These functions support BOTH calling styles:
#   OLD: log_pipeline_start("PipelineName", details)
#   NEW: log_pipeline_start(logger, "PipelineName", details)
# -------------------------------------------------------
def log_pipeline_start(logger_or_name, pipeline_name=None, details=None):
    if isinstance(logger_or_name, str):
        # Old style: log_pipeline_start("name", details)
        name    = logger_or_name
        details = pipeline_name  # second arg is actually details
        print(f"\n{'='*55}")
        print(f"  PIPELINE : {name}")
        print(f"  STARTED  : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        if details:
            for k, v in details.items():
                print(f"  {k:15} : {v}")
        print(f"{'='*55}")
    else:
        # New style: log_pipeline_start(logger, "name", details)
        logger = logger_or_name
        logger.info("=" * 55)
        logger.info(f"PIPELINE STARTED : {pipeline_name}")
        if details:
            for k, v in details.items():
                logger.info(f"  {k:15} : {v}")
        logger.info("=" * 55)


def log_pipeline_end(logger_or_name, pipeline_name=None, results=None):
    if isinstance(logger_or_name, str):
        name    = logger_or_name
        results = pipeline_name
        print(f"\n{'='*55}")
        print(f"  PIPELINE  : {name}")
        print(f"  COMPLETED : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        if results:
            for k, v in results.items():
                print(f"  {k:15} : {v}")
        print(f"{'='*55}\n")
    else:
        logger = logger_or_name
        logger.info("=" * 55)
        logger.info(f"PIPELINE COMPLETE : {pipeline_name}")
        if results:
            for k, v in results.items():
                logger.info(f"  {k:15} : {v}")
        logger.info("=" * 55)


def log_step(step_name, message):
    print(f"  [{step_name}] {message}")


def log_error(step_name, error):
    print(f"  [ERROR - {step_name}] {str(error)}")


def log_api_request(logger, endpoint, params=None):
    logger.info(f"API REQUEST  : {endpoint}")
    if params:
        logger.debug(f"  params: {params}")


def log_api_response(logger, endpoint, count):
    logger.info(f"API RESPONSE : {endpoint} -> {count} records")


def log_api_error(logger, endpoint, error, video_id=None):
    logger.error(
        f"API ERROR    : {endpoint} | "
        f"video_id={video_id} | error={str(error)}"
    )


def save_lineage(lineage_path, record):
    os.makedirs(lineage_path, exist_ok=True)
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = os.path.join(lineage_path, f"lineage_{today}.json")
    existing = []
    if os.path.exists(filename):
        with open(filename, "r") as f:
            existing = json.load(f)
    record["logged_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    existing.append(record)
    with open(filename, "w") as f:
        json.dump(existing, f, indent=2)
