"""Make installing brasa-marketdata 0.5.0 fail with a message (renamed package)."""

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

MESSAGE = (
    "\n\nbrasa-marketdata was renamed to brasa-core (0.5.0).\n"
    "Run:  pip uninstall brasa-marketdata && pip install brasa-core\n"
)


class CustomBuildHook(BuildHookInterface):
    """Stop every wheel build with the rename message."""

    def initialize(self, version: str, build_data: dict) -> None:  # noqa: ARG002
        """Raise before anything is built.

        Args:
            version: The wheel version being built (unused).
            build_data: Hatchling build data (unused).
        """
        raise SystemExit(MESSAGE)
