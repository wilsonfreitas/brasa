"""Allow ``python -m brasa`` — used by the orchestrator's subprocess isolation."""

from brasa.cli import main

if __name__ == "__main__":
    main()
