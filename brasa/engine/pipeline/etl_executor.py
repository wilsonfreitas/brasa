"""ETL Pipeline executor for running dataset transformation steps.

This module provides the ETLPipeline class that executes a sequence
of ETL steps and writes the result to the output dataset.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq

from brasa.engine.cache import CacheManager
from brasa.fieldsets.adapters import PyArrowAdapter

from .etl_context import ETLPipelineContext
from .registry import StepRegistry
from .step import PipelineStep

if TYPE_CHECKING:
    from brasa.engine.template import MarketDataWriter
    from brasa.fieldsets import Fieldset

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Executes a sequence of ETL pipeline steps.

    The ETL pipeline loads input datasets, applies transformations,
    and writes the result to an output dataset. The output partitioning
    is controlled by the writer configuration.

    Attributes:
        steps: List of ETL pipeline steps to execute.
    """

    def __init__(self, steps: list[PipelineStep]) -> None:
        """Initialize the pipeline with a list of steps.

        Args:
            steps: Ordered list of steps to execute.
        """
        self.steps = steps

    @classmethod
    def from_config(cls, config: list[dict[str, Any]]) -> ETLPipeline:
        """Create a pipeline from YAML configuration.

        Args:
            config: List of step configurations from YAML.

        Returns:
            Configured ETLPipeline instance.

        Raises:
            ValueError: If any step configuration is invalid.
        """
        steps = []
        for step_config in config:
            if not isinstance(step_config, dict):
                raise ValueError(
                    f"Invalid step configuration type: {type(step_config)}"
                )
            if "step" not in step_config:
                raise ValueError(
                    f"Step configuration must have a 'step' key: {step_config}"
                )
            step_name = step_config["step"]
            step = StepRegistry.create(step_name, step_config)
            steps.append(step)
        return cls(steps)

    def get_input_datasets(self) -> list[str]:
        """Get all input dataset names that this pipeline depends on.

        Returns:
            List of unique dataset names that are inputs to this pipeline.
        """
        datasets = set()
        for step in self.steps:
            datasets.update(step.get_input_datasets())
        return list(datasets)

    def execute(
        self,
        template_id: str,
        writer: MarketDataWriter | None = None,
        fields: Fieldset | None = None,
    ) -> pd.DataFrame:
        """Execute the pipeline and return the result.

        Args:
            template_id: The ID of the template being processed.
            writer: Writer configuration for output.
            fields: Optional field definitions for schema.

        Returns:
            DataFrame with the processed data.
        """
        context = ETLPipelineContext(
            template_id=template_id,
            writer=writer,
            fields=fields,
        )

        data: Any = None
        for i, step in enumerate(self.steps):
            step_name = step.name or step.__class__.__name__
            logger.debug(f"Executing ETL step {i + 1}/{len(self.steps)}: {step_name}")
            try:
                data = step.execute(data, context)
            except Exception as e:
                logger.error(f"ETL Step '{step_name}' failed: {e}")
                raise RuntimeError(
                    f"ETL Pipeline failed at step {i + 1} ({step_name}): {e}"
                ) from e

        # Convert to DataFrame if needed
        if isinstance(data, pads.Dataset):
            data = data.to_table().to_pandas()
        elif hasattr(data, "to_pandas") and not isinstance(data, pd.DataFrame):
            data = data.to_pandas()

        return data

    def execute_and_write(
        self,
        template_id: str,
        writer: MarketDataWriter | None = None,
        fields: Fieldset | None = None,
    ) -> None:
        """Execute the pipeline and write the result to the output dataset.

        Args:
            template_id: The ID of the template (used as output dataset name).
            writer: Writer configuration for output partitioning.
            fields: Optional field definitions for schema enforcement.
        """
        df = self.execute(template_id, writer, fields)
        man = CacheManager()

        # Get output path using layer and dataset from writer
        if writer is not None:
            layer = writer.layer.value
            dataset = writer.dataset
            output_path = man.db_path(f"{layer}/{dataset}")
        else:
            # Fallback for templates without writer config
            from brasa.engine.layers import DEFAULT_ETL_LAYER

            layer = DEFAULT_ETL_LAYER.value
            dataset = template_id
            output_path = man.db_path(f"{layer}/{template_id}")

        # Get partitioning from writer
        partitioning = []
        if writer is not None:
            partitioning = getattr(writer, "partitioning", [])

        # Get schema from fields if available
        schema = None
        if fields is not None:
            try:
                adapter = PyArrowAdapter(fields, verbose_warnings=False)
                schema = adapter.get_target_schema()
            except Exception:
                pass

        # Convert DataFrame to PyArrow Table
        if schema:
            table = pa.Table.from_pandas(df, schema=schema)
        else:
            table = pa.Table.from_pandas(df)

        # Write the dataset
        from pathlib import Path

        # Ensure output directory exists
        Path(output_path).mkdir(parents=True, exist_ok=True)

        if partitioning:
            pq.write_to_dataset(
                table,
                root_path=output_path,
                partition_cols=partitioning,
                existing_data_behavior="delete_matching",
            )
        else:
            # Write as a single file
            pq.write_table(table, f"{output_path}/data.parquet")

        # Register dataset in catalog
        from brasa.engine.catalog import DatasetCatalog

        catalog = DatasetCatalog()
        catalog.register_dataset(
            layer=layer,
            dataset_name=dataset,
            schema=table.schema,
            partitioning=partitioning if partitioning else [],
            source_template=template_id,
        )

        logger.info(f"Wrote ETL output to {output_path}")

        from brasa.engine.dependency_resolver import _touch_marker

        _touch_marker(output_path)

    def __repr__(self) -> str:
        step_names = [s.name or s.__class__.__name__ for s in self.steps]
        return f"ETLPipeline(steps={step_names})"

    def __len__(self) -> int:
        return len(self.steps)
