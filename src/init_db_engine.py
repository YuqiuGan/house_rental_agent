from typing import Optional
from threading import Lock
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os
from src.settings import DATABASE_URL

# DATABASE_URL = os.getenv("DATABASE_URL")
_engine: Optional[Engine] = None
_lock = Lock()

def get_engine() -> Engine:
    global _engine
    eng = _engine
    if eng is None:
        with _lock:
            eng = _engine
            if eng is None:
                eng = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
                _engine = eng
    return eng

def set_database_url(url: str) -> None:
    global DATABASE_URL, _engine
    with _lock:
        DATABASE_URL = url
        if _engine:
            _engine.dispose()
        _engine = None
