# naming.py
from __future__ import annotations
import os

def fc(gdb: str, name: str) -> str:
    """Bygg full sti til en feature class i angitt GDB."""
    return os.path.join(gdb, name)