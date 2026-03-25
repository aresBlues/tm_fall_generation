"""CLI entry: `python run.py` (same as `python -m src.main`).

Examples:
  python run.py
  python run.py -n 50
  python run.py --per-alert
"""
from src.main import main

if __name__ == "__main__":
    main()
