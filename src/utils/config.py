from functools import lru_cache
from pathlib import Path
from typing import Optional, Union

from dotenv import load_dotenv


@lru_cache(maxsize=1)
def load_project_dotenv(dotenv_path: Optional[Union[str, Path]] = None) -> bool:
    """
    Загружает .env из корня проекта, если он существует.
    Не перезаписывает уже заданные переменные окружения.
    """
    env_path = Path(dotenv_path) if dotenv_path else Path(__file__).resolve().parents[2] / ".env"

    if not env_path.exists():
        return False

    return load_dotenv(env_path, override=False)
