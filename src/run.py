import sys
from argparse import ArgumentParser

from loguru import logger

from src.utils.parser import Parser


def get_pipeline_args():
    parser = ArgumentParser(description="Kaliningradka")
    parser.add_argument(
        "-t",
        "--task",
        required=True,
        default=None,
        type=str,
        choices=["parser"],
        help="The task to perform.",
    )
    parser.add_argument(
        "-l",
        "--log",
        default="DEBUG",
        type=str.upper,
        dest="log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging output level",
    )
    return parser.parse_args()


def entrypoint(model, task, **kwargs):
    """
    This method will handle the pipeline for the given state, and after that will launch that pipeline.
    """
    task_selector = {
        "parser": {
            "get_links": Parser.get_links,
            "get_images": Parser.get_images,
        }
    }
    task = task_selector[task][subtask]
    task(**kwargs).run()


if __name__ == "__main__":
    args = get_pipeline_args()

    logger.remove()
    logger.add(sys.stdout, level=args.log_level)

    entrypoint(
        env=args.env,
        model=args.model,
        task=args.task,
        task_config_path=args.task_conf_file,
        base_config_path=args.base_conf_file,
    )
