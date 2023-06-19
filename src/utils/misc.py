from pathlib import Path


def get_project_root() -> Path:
    """Returns project root folder.
    Note:
        In any module in the current project get the project root as follows:
            from src.utils.misc import get_project_root
            root = get_project_root()
        Any module which calls get_project_root can be moved without changing program behavior.
    """
    return Path(__file__).parents[2]
