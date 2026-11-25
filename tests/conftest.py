import sys
from pathlib import Path

# make pipeline/ modules importable as top-level modules (same as when run.py runs)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline"))
