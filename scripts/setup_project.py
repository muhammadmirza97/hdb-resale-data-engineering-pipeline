"""
One-time project setup script.
Creates all required directories and adds .gitkeep files.

Usage (from the project root):
  python scripts/setup_project.py
"""

from pathlib import Path

DIRS = [
    "data/raw",
    "data/processed",
    "data/curated",
    "warehouse",
    "reports",
    "logs",
    "notebooks",
    "src",
    "dags",
    "scripts",
    "config",
]


def main() -> None:
    print("Setting up project directories...\n")
    for d in DIRS:
        p = Path(d)
        p.mkdir(parents=True, exist_ok=True)
        gitkeep = p / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
        print(f"  [OK] {d}/")
    print("\nSetup complete. Run the pipeline with:")
    print("  python -m src.run_pipeline")


if __name__ == "__main__":
    main()
