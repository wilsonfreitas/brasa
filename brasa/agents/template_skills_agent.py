"""Template skills agent for converting ETL functions to pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from brasa.engine.pipeline import StepRegistry


@dataclass
class PipelineOption:
    """Represents a proposed pipeline option for a template."""

    steps: list[dict[str, Any]]
    writer_partitioning: list[str]
    rationale: str
    valid: bool
    errors: list[str]


@dataclass
class ConversionProposal:
    """Represents a conversion proposal for a template."""

    template_id: str
    template_path: Path
    etl_function: str
    input_datasets: list[str]
    options: list[PipelineOption]
    skipped: bool
    skip_reason: str | None


class TemplateConversionAgent:
    """Convert ETL function templates into pipeline templates."""

    def __init__(self, mapping_dir: Path | str) -> None:
        self.mapping_dir = Path(mapping_dir)
        self.default_partitioning = ["refdate"]

    def propose(self, template_path: Path | str) -> ConversionProposal:
        """Build pipeline proposals for a template.

        Args:
            template_path: Path to the YAML template.

        Returns:
            ConversionProposal with one or more pipeline options.
        """
        template_path = Path(template_path)
        template = self._load_template(template_path)
        template_id = template.get("id", "")
        etl_config = template.get("etl")

        if not etl_config:
            return ConversionProposal(
                template_id=template_id,
                template_path=template_path,
                etl_function="",
                input_datasets=[],
                options=[],
                skipped=True,
                skip_reason="template has no etl section",
            )

        if "pipeline" in etl_config:
            return ConversionProposal(
                template_id=template_id,
                template_path=template_path,
                etl_function="",
                input_datasets=[],
                options=[],
                skipped=True,
                skip_reason="template already uses etl.pipeline",
            )

        etl_function = etl_config.get("function")
        if not etl_function:
            return ConversionProposal(
                template_id=template_id,
                template_path=template_path,
                etl_function="",
                input_datasets=[],
                options=[],
                skipped=True,
                skip_reason="etl.function not found",
            )

        etl_args = {
            key: value for key, value in etl_config.items() if key != "function"
        }
        input_datasets = self._extract_input_datasets(etl_config)
        load_steps = self._build_load_steps(input_datasets)

        steps = [
            *load_steps,
            {
                "step": "legacy_etl_output",
                "function": etl_function,
                "args": etl_args,
            },
        ]

        option = self._validate_option(
            PipelineOption(
                steps=steps,
                writer_partitioning=list(self.default_partitioning),
                rationale=("Fallback proposal using legacy ETL function output."),
                valid=True,
                errors=[],
            )
        )

        return ConversionProposal(
            template_id=template_id,
            template_path=template_path,
            etl_function=etl_function,
            input_datasets=input_datasets,
            options=[option],
            skipped=False,
            skip_reason=None,
        )

    def apply(
        self,
        proposal: ConversionProposal,
        option_index: int,
    ) -> None:
        """Apply a selected proposal to the template and write mapping.

        Args:
            proposal: The conversion proposal to apply.
            option_index: Index of the chosen option.
        """
        if proposal.skipped:
            raise ValueError("Cannot apply a skipped proposal")

        if option_index >= len(proposal.options):
            raise IndexError("Option index out of range")

        option = proposal.options[option_index]
        if not option.valid:
            raise ValueError("Selected option is invalid")

        template = self._load_template(proposal.template_path)
        template_id = template.get("id", "")

        template["etl"] = {"pipeline": option.steps}

        writer = template.get("writer", {})
        writer["partitioning"] = option.writer_partitioning
        template["writer"] = writer

        self._write_template(proposal.template_path, template)
        self._write_mapping_file(
            template_id=template_id,
            etl_function=proposal.etl_function,
            partitioning=option.writer_partitioning,
            rationale=option.rationale,
        )

    def _load_template(self, template_path: Path) -> dict[str, Any]:
        """Load a YAML template from disk."""
        with template_path.open(encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def _write_template(self, template_path: Path, template: dict[str, Any]) -> None:
        """Write the updated template back to disk."""
        content = yaml.safe_dump(
            template,
            sort_keys=False,
            allow_unicode=True,
        )
        template_path.write_text(content, encoding="utf-8")

    def _extract_input_datasets(self, etl_config: dict[str, Any]) -> list[str]:
        """Extract dataset references from etl configuration."""
        datasets: list[str] = []
        for key, value in etl_config.items():
            if key == "function":
                continue
            if not isinstance(value, str):
                continue
            if key.endswith("_dataset"):
                datasets.append(value)
        return datasets

    def _build_load_steps(self, datasets: list[str]) -> list[dict[str, Any]]:
        """Build load + store steps for datasets."""
        steps: list[dict[str, Any]] = []
        for dataset in datasets:
            steps.append({"step": "load", "template": dataset})
            steps.append({"step": "store_result", "name": dataset})
        return steps

    def _validate_option(self, option: PipelineOption) -> PipelineOption:
        """Validate a pipeline option using the registry."""
        errors: list[str] = []
        for step in option.steps:
            step_name = step.get("step")
            if not step_name:
                errors.append("step missing 'step' key")
                continue
            if StepRegistry.get(step_name) is None:
                errors.append(f"unknown step: {step_name}")
                continue
            try:
                StepRegistry.create(step_name, step)
            except ValueError as exc:
                errors.append(str(exc))

        option.errors = errors
        option.valid = len(errors) == 0
        return option

    def _write_mapping_file(
        self,
        template_id: str,
        etl_function: str,
        partitioning: list[str],
        rationale: str,
    ) -> None:
        """Write a mapping file for the converted template."""
        mapping_path = self.mapping_dir / f"{template_id}.yaml"
        mapping_path.parent.mkdir(parents=True, exist_ok=True)

        header = (
            "# Mapping generated by template skills agent.\n"
            f"# etl.function: {etl_function}\n"
        )
        payload = {
            "template_id": template_id,
            "version": 1,
            "updated_at": date.today().isoformat(),
            "partitioning": partitioning,
            "rationale": rationale,
            "confidence": 0.5,
            "mappings": [],
        }
        body = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
        mapping_path.write_text(f"{header}{body}", encoding="utf-8")
