"""Template compiler for resolving inheritance and generating expanded YAML files.

This module implements a preprocessor that resolves `extends` references in template
files and produces fully-expanded YAML templates. The compiler supports:

- Recursive inheritance resolution (grandparent → parent → child)
- Deep merge for nested mappings (dicts)
- Keyed merge for lists (fields, datasets, pipeline steps)
- Circular dependency detection
- Backward compatibility (templates without `extends` pass through unchanged)
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml


class TemplateInheritanceError(Exception):
    """Raised when template inheritance cannot be resolved."""

    pass


class CircularInheritanceError(TemplateInheritanceError):
    """Raised when circular inheritance is detected."""

    pass


class TemplateNotFoundError(TemplateInheritanceError):
    """Raised when a parent template cannot be found."""

    pass


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries, with override values taking precedence.

    Args:
        base: Base dictionary
        override: Dictionary with override values

    Returns:
        Merged dictionary
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _keyed_merge(
    base: list[dict[str, Any]],
    override: list[dict[str, Any]],
    key: str,
) -> list[dict[str, Any]]:
    """Merge two lists of dictionaries by a key field.

    Items with the same key are deep-merged; unique items are appended.

    Args:
        base: Base list of dictionaries
        override: Override list of dictionaries
        key: Key field name to use for matching

    Returns:
        Merged list of dictionaries
    """
    result_map: dict[Any, dict[str, Any]] = {}

    # Add all base items
    for item in base:
        if key in item:
            result_map[item[key]] = copy.deepcopy(item)
        else:
            # Items without key are always included (edge case)
            result_map[id(item)] = copy.deepcopy(item)

    # Merge or add override items
    for item in override:
        if key in item:
            item_key = item[key]
            if item_key in result_map:
                # Merge with existing item
                result_map[item_key] = _deep_merge(result_map[item_key], item)
            else:
                # Add new item
                result_map[item_key] = copy.deepcopy(item)
        else:
            # Items without key are always added
            result_map[id(item)] = copy.deepcopy(item)

    return list(result_map.values())


def _merge_templates(parent: dict[str, Any], child: dict[str, Any]) -> dict[str, Any]:
    """Merge parent and child templates with intelligent field handling.

    Args:
        parent: Parent template configuration
        child: Child template configuration

    Returns:
        Merged template configuration
    """
    result = _deep_merge(parent, child)

    # Special handling for keyed lists
    # 1. Merge fields by name
    if "fields" in parent and "fields" in child:
        result["fields"] = _keyed_merge(parent["fields"], child["fields"], "name")

    # 2. Merge datasets by dataset key
    if "datasets" in parent and "datasets" in child:
        result["datasets"] = {}
        # Start with parent datasets
        for ds_key, ds_config in parent["datasets"].items():
            result["datasets"][ds_key] = copy.deepcopy(ds_config)
        # Merge/add child datasets
        for ds_key, ds_config in child["datasets"].items():
            if ds_key in result["datasets"]:
                # Merge dataset fields if both have fields
                merged_ds = _deep_merge(result["datasets"][ds_key], ds_config)
                if "fields" in result["datasets"][ds_key] and "fields" in ds_config:
                    merged_ds["fields"] = _keyed_merge(
                        result["datasets"][ds_key]["fields"],
                        ds_config["fields"],
                        "name",
                    )
                result["datasets"][ds_key] = merged_ds
            else:
                result["datasets"][ds_key] = copy.deepcopy(ds_config)

    # 3. Merge reader pipeline by step
    if (
        "reader" in parent
        and "pipeline" in parent["reader"]
        and "reader" in child
        and "pipeline" in child["reader"]
    ):
        result["reader"]["pipeline"] = _keyed_merge(
            parent["reader"]["pipeline"],
            child["reader"]["pipeline"],
            "step",
        )

    # 4. Merge etl pipeline by step
    if (
        "etl" in parent
        and "pipeline" in parent["etl"]
        and "etl" in child
        and "pipeline" in child["etl"]
    ):
        result["etl"]["pipeline"] = _keyed_merge(
            parent["etl"]["pipeline"],
            child["etl"]["pipeline"],
            "step",
        )

    return result


class TemplateCompiler:
    """Compiles templates by resolving inheritance chains.

    The compiler loads template YAML files, resolves `extends` references
    recursively, and produces fully-expanded templates.
    """

    def __init__(self, templates_dir: Path):
        """Initialize the compiler.

        Args:
            templates_dir: Directory containing template YAML files
        """
        self.templates_dir = Path(templates_dir)
        self._template_cache: dict[str, dict[str, Any]] = {}
        self._resolution_stack: list[str] = []

    def _load_raw_template(self, template_id: str) -> dict[str, Any]:
        """Load a raw template from disk without resolving inheritance.

        Args:
            template_id: Template identifier

        Returns:
            Raw template configuration

        Raises:
            TemplateNotFoundError: If template file doesn't exist
        """
        if template_id in self._template_cache:
            return self._template_cache[template_id]

        template_path = self.templates_dir / f"{template_id}.yaml"
        if not template_path.exists():
            msg = f"Template '{template_id}' not found at {template_path}"
            raise TemplateNotFoundError(msg)

        with template_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        self._template_cache[template_id] = config
        return config

    def _resolve_inheritance(self, template_id: str) -> dict[str, Any]:
        """Recursively resolve template inheritance.

        Args:
            template_id: Template identifier to resolve

        Returns:
            Fully resolved template configuration

        Raises:
            CircularInheritanceError: If circular inheritance detected
            TemplateNotFoundError: If parent template not found
        """
        # Check for circular references
        if template_id in self._resolution_stack:
            chain = " → ".join([*self._resolution_stack, template_id])
            msg = f"Circular inheritance detected: {chain}"
            raise CircularInheritanceError(msg)

        # Load raw template
        config = self._load_raw_template(template_id)

        # If no extends, return as-is
        if "extends" not in config:
            return copy.deepcopy(config)

        # Resolve parent first (recursive)
        parent_id = config["extends"]
        self._resolution_stack.append(template_id)
        try:
            parent_config = self._resolve_inheritance(parent_id)
        finally:
            self._resolution_stack.pop()

        # Merge parent and child
        merged = _merge_templates(parent_config, config)

        # Remove extends from final output
        if "extends" in merged:
            del merged["extends"]

        return merged

    def compile_template(self, template_id: str) -> dict[str, Any]:
        """Compile a single template by resolving inheritance.

        Args:
            template_id: Template identifier

        Returns:
            Compiled template configuration

        Raises:
            TemplateInheritanceError: If compilation fails
        """
        # Reset resolution stack for each top-level compilation
        self._resolution_stack = []
        return self._resolve_inheritance(template_id)

    def compile_all(
        self,
        output_dir: Path | None = None,
        template_ids: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Compile all templates or a subset.

        Args:
            output_dir: Optional directory to write compiled YAML files
            template_ids: Optional list of template IDs to compile (default: all)

        Returns:
            Dictionary mapping template IDs to compiled configurations
        """
        if template_ids is None:
            # Compile all templates in directory
            template_files = self.templates_dir.glob("*.yaml")
            template_ids = [f.stem for f in template_files]

        compiled = {}
        for template_id in template_ids:
            try:
                compiled[template_id] = self.compile_template(template_id)
            except TemplateInheritanceError as e:
                print(f"Warning: Failed to compile '{template_id}': {e}")
                continue

        # Write to output directory if specified
        if output_dir is not None:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            for template_id, config in compiled.items():
                output_path = output_dir / f"{template_id}.yaml"
                with output_path.open("w", encoding="utf-8") as f:
                    yaml.safe_dump(
                        config,
                        f,
                        default_flow_style=False,
                        sort_keys=False,
                        allow_unicode=True,
                    )

        return compiled


def compile_templates(
    templates_dir: Path,
    output_dir: Path,
    template_ids: list[str] | None = None,
) -> int:
    """Compile templates and write to output directory.

    Args:
        templates_dir: Source templates directory
        output_dir: Destination for compiled templates
        template_ids: Optional list of specific templates to compile

    Returns:
        Number of successfully compiled templates
    """
    compiler = TemplateCompiler(templates_dir)
    compiled = compiler.compile_all(output_dir, template_ids)
    return len(compiled)
