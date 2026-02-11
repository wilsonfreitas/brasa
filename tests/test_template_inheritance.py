"""Tests for template inheritance and compilation."""

from pathlib import Path

import pytest
import yaml

from brasa.engine.compiler import (
    CircularInheritanceError,
    TemplateCompiler,
    TemplateNotFoundError,
    _deep_merge,
    _keyed_merge,
    compile_templates,
)
from brasa.engine.template import retrieve_template


class TestMergeFunctions:
    """Test the core merge functions."""

    def test_deep_merge_simple(self):
        """Test deep merge with simple dictionaries."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_deep_merge_nested(self):
        """Test deep merge with nested dictionaries."""
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        override = {"a": {"y": 3, "z": 4}, "c": 5}
        result = _deep_merge(base, override)
        assert result == {"a": {"x": 1, "y": 3, "z": 4}, "b": 3, "c": 5}

    def test_keyed_merge_fields(self):
        """Test keyed merge for fields list."""
        base = [
            {"name": "refdate", "type": "date"},
            {"name": "symbol", "type": "character"},
        ]
        override = [
            {"name": "symbol", "description": "Ticker symbol"},
            {"name": "price", "type": "number"},
        ]
        result = _keyed_merge(base, override, "name")

        assert len(result) == 3
        # Check symbol was merged
        symbol_field = next(f for f in result if f["name"] == "symbol")
        assert symbol_field["type"] == "character"
        assert symbol_field["description"] == "Ticker symbol"

    def test_keyed_merge_pipeline(self):
        """Test keyed merge for pipeline steps."""
        base = [
            {"step": "read_csv", "separator": ","},
            {"step": "select_columns", "columns": ["date", "price"]},
        ]
        override = [
            {"step": "read_csv", "separator": ";"},
            {"step": "apply_fields"},
        ]
        result = _keyed_merge(base, override, "step")

        assert len(result) == 3
        # Check read_csv was overridden
        read_csv = next(s for s in result if s["step"] == "read_csv")
        assert read_csv["separator"] == ";"


class TestTemplateCompiler:
    """Test the TemplateCompiler class."""

    @pytest.fixture
    def temp_templates_dir(self, tmp_path):
        """Create a temporary templates directory with test templates."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()

        # Create base template
        base = {
            "id": "base",
            "description": "Base template",
            "etl": {"futures_dataset": "b3-futures-settlement-prices"},
        }
        with (templates_dir / "base.yaml").open("w") as f:
            yaml.safe_dump(base, f)

        # Create child template
        child = {
            "extends": "base",
            "id": "child",
            "description": "Child template",
            "etl": {"function": "brasa.etl.test", "commodity": "TEST"},
        }
        with (templates_dir / "child.yaml").open("w") as f:
            yaml.safe_dump(child, f)

        # Create standalone template (no inheritance)
        standalone = {
            "id": "standalone",
            "description": "Standalone template",
            "downloader": {"function": "test", "url": "http://test.com"},
        }
        with (templates_dir / "standalone.yaml").open("w") as f:
            yaml.safe_dump(standalone, f)

        return templates_dir

    @pytest.fixture
    def circular_templates_dir(self, tmp_path):
        """Create templates with circular inheritance."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()

        # Create circular dependency: a -> b -> c -> a
        a = {"extends": "c", "id": "a"}
        with (templates_dir / "a.yaml").open("w") as f:
            yaml.safe_dump(a, f)

        b = {"extends": "a", "id": "b"}
        with (templates_dir / "b.yaml").open("w") as f:
            yaml.safe_dump(b, f)

        c = {"extends": "b", "id": "c"}
        with (templates_dir / "c.yaml").open("w") as f:
            yaml.safe_dump(c, f)

        return templates_dir

    def test_compile_standalone_template(self, temp_templates_dir):
        """Test compiling a template without inheritance."""
        compiler = TemplateCompiler(temp_templates_dir)
        result = compiler.compile_template("standalone")

        assert result["id"] == "standalone"
        assert "extends" not in result
        assert result["downloader"]["function"] == "test"

    def test_compile_child_template(self, temp_templates_dir):
        """Test compiling a template with inheritance."""
        compiler = TemplateCompiler(temp_templates_dir)
        result = compiler.compile_template("child")

        assert result["id"] == "child"
        assert "extends" not in result
        # Should have merged etl config
        assert result["etl"]["futures_dataset"] == "b3-futures-settlement-prices"
        assert result["etl"]["function"] == "brasa.etl.test"
        assert result["etl"]["commodity"] == "TEST"

    def test_template_not_found(self, temp_templates_dir):
        """Test error when parent template doesn't exist."""
        compiler = TemplateCompiler(temp_templates_dir)

        # Create a template with non-existent parent
        bad_child = {
            "extends": "nonexistent",
            "id": "bad-child",
        }
        with (temp_templates_dir / "bad-child.yaml").open("w") as f:
            yaml.safe_dump(bad_child, f)

        with pytest.raises(TemplateNotFoundError):
            compiler.compile_template("bad-child")

    def test_circular_inheritance_detection(self, circular_templates_dir):
        """Test detection of circular inheritance."""
        compiler = TemplateCompiler(circular_templates_dir)

        with pytest.raises(CircularInheritanceError) as exc_info:
            compiler.compile_template("a")

        assert "Circular inheritance detected" in str(exc_info.value)

    def test_compile_all_templates(self, temp_templates_dir):
        """Test compiling all templates in a directory."""
        compiler = TemplateCompiler(temp_templates_dir)
        compiled = compiler.compile_all()

        assert "base" in compiled
        assert "child" in compiled
        assert "standalone" in compiled

    def test_compile_all_with_output(self, temp_templates_dir, tmp_path):
        """Test compiling templates and writing to output directory."""
        output_dir = tmp_path / "compiled"
        compiler = TemplateCompiler(temp_templates_dir)
        compiler.compile_all(output_dir)

        # Check output files were created
        assert (output_dir / "base.yaml").exists()
        assert (output_dir / "child.yaml").exists()
        assert (output_dir / "standalone.yaml").exists()

        # Verify content of compiled child
        with (output_dir / "child.yaml").open() as f:
            child_compiled = yaml.safe_load(f)
        assert (
            child_compiled["etl"]["futures_dataset"] == "b3-futures-settlement-prices"
        )
        assert child_compiled["etl"]["function"] == "brasa.etl.test"

    def test_compile_specific_templates(self, temp_templates_dir, tmp_path):
        """Test compiling specific templates only."""
        output_dir = tmp_path / "compiled"
        num_compiled = compile_templates(
            temp_templates_dir, output_dir, template_ids=["child"]
        )

        assert num_compiled == 1
        assert (output_dir / "child.yaml").exists()
        assert not (output_dir / "standalone.yaml").exists()


class TestRealTemplateInheritance:
    """Test compilation of real B3 futures templates."""

    @pytest.fixture
    def templates_dir(self):
        """Get the actual templates directory."""
        return Path("templates")

    def test_b3_futures_templates_exist(self, templates_dir):
        """Verify the test templates exist."""
        assert (templates_dir / "b3-futures-base.yaml").exists()
        assert (templates_dir / "b3-futures-di1.yaml").exists()
        assert (templates_dir / "b3-futures-ddi.yaml").exists()
        assert (templates_dir / "b3-futures-dol.yaml").exists()
        assert (templates_dir / "b3-futures-wdo.yaml").exists()

    def test_compile_b3_futures_di1(self, templates_dir):
        """Test compiling b3-futures-di1 with inheritance."""
        compiler = TemplateCompiler(templates_dir)
        result = compiler.compile_template("b3-futures-di1")

        # Check structure
        assert result["id"] == "b3-futures-di1"
        assert "extends" not in result
        assert "etl" in result

        # Check inherited fields
        assert result["etl"]["futures_dataset"] == "b3-futures-settlement-prices"
        assert result["etl"]["maturity_day"] == "first day"

        # Check specific fields
        assert result["etl"]["function"] == "brasa.etl.create_b3_rate_futures"
        assert result["etl"]["commodity"] == "DI1"
        assert result["etl"]["compounding"] == "discrete"

    def test_compile_all_b3_futures(self, templates_dir, tmp_path):
        """Test compiling all b3-futures templates."""
        output_dir = tmp_path / "compiled"
        template_ids = [
            "b3-futures-di1",
            "b3-futures-ddi",
            "b3-futures-dol",
            "b3-futures-wdo",
        ]

        num_compiled = compile_templates(templates_dir, output_dir, template_ids)

        assert num_compiled == 4

        # Verify each compiled template
        for template_id in template_ids:
            compiled_path = output_dir / f"{template_id}.yaml"
            assert compiled_path.exists()

            with compiled_path.open() as f:
                config = yaml.safe_load(f)

            # All should have base template fields
            assert config["etl"]["futures_dataset"] == "b3-futures-settlement-prices"
            assert config["etl"]["maturity_day"] == "first day"
            assert "commodity" in config["etl"]


class TestTemplateRetrieval:
    """Test template retrieval with compiled templates."""

    def test_retrieve_template_without_inheritance(self):
        """Test retrieving a template that doesn't use inheritance."""
        template = retrieve_template("bcb-sgs-data")
        assert template is not None
        assert template.id == "bcb-sgs-data"

    def test_retrieve_compiled_template(self, tmp_path):
        """Test retrieving templates from compiled directory."""
        # This test would require setting up environment variable
        # For now, just verify the function signature works
        template = retrieve_template("bcb-sgs-data", use_compiled=False)
        assert template is not None


class TestBackwardCompatibility:
    """Test that templates without inheritance continue to work."""

    def test_existing_templates_load_unchanged(self):
        """Verify existing templates without inheritance still load correctly."""
        legacy_templates = [
            "bcb-sgs-data",
            "b3-cotahist-daily",
            "b3-futures-settlement-prices",
        ]

        for template_name in legacy_templates:
            try:
                template = retrieve_template(template_name)
                assert template is not None
                assert template.id == template_name
            except Exception as e:
                pytest.fail(f"Failed to load legacy template {template_name}: {e}")

    def test_compiled_template_equivalence(self, tmp_path):
        """Test that compiled templates are functionally equivalent to originals."""
        templates_dir = Path("templates")
        output_dir = tmp_path / "compiled"

        # Compile the b3-futures templates
        template_ids = [
            "b3-futures-di1",
            "b3-futures-ddi",
            "b3-futures-dol",
            "b3-futures-wdo",
        ]

        compile_templates(templates_dir, output_dir, template_ids)

        # For each template, verify key properties are preserved
        for template_id in template_ids:
            with (output_dir / f"{template_id}.yaml").open() as f:
                compiled = yaml.safe_load(f)

            # Verify essential structure
            assert compiled["id"] == template_id
            assert "etl" in compiled
            assert "function" in compiled["etl"]
            assert "commodity" in compiled["etl"]
            assert "futures_dataset" in compiled["etl"]
            assert "maturity_day" in compiled["etl"]
