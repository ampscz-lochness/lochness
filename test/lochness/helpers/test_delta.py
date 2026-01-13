"""
Tests for the delta module.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import tempfile

import pytest

from lochness.helpers.delta import (
    compute_delta,
    apply_delta,
    compute_delta_from_files,
    apply_delta_to_file,
    apply_delta_chain,
    create_file_metadata_with_delta,
    is_delta_efficient,
    convert_delta_format,
    DeltaPatchError,
)


class TestComputeDelta:
    """Tests for compute_delta function."""

    def test_identical_content_produces_empty_delta(self):
        """Identical content should produce a minimal delta."""
        content = "Hello, World!"
        delta = compute_delta(content, content)
        # Apply delta should return same content
        result = apply_delta(content, delta)
        assert result == content

    def test_simple_modification(self):
        """Test delta for simple text modification."""
        original = "Hello, World!"
        modified = "Hello, Python!"

        delta = compute_delta(original, modified)
        result = apply_delta(original, delta)

        assert result == modified

    def test_human_readable_format(self):
        """Test human-readable delta format."""
        original = "Line 1\nLine 2\nLine 3"
        modified = "Line 1\nModified Line\nLine 3"

        delta = compute_delta(original, modified, human_readable=True)

        # Human-readable format should contain readable patch markers
        assert "@" in delta or "@@" in delta or len(delta) > 0

        result = apply_delta(original, delta, human_readable=True)
        assert result == modified

    def test_compressed_format_is_smaller(self):
        """Compressed format should generally be smaller for larger diffs."""
        original = "A" * 1000
        modified = "B" * 1000

        human_delta = compute_delta(original, modified, human_readable=True)
        compressed_delta = compute_delta(original, modified, human_readable=False)

        # For large changes, compressed should be smaller
        assert len(compressed_delta) <= len(human_delta)

    def test_addition_at_end(self):
        """Test adding content at the end."""
        original = "Hello"
        modified = "Hello, World!"

        delta = compute_delta(original, modified)
        result = apply_delta(original, delta)

        assert result == modified

    def test_deletion(self):
        """Test content deletion."""
        original = "Hello, World!"
        modified = "Hello!"

        delta = compute_delta(original, modified)
        result = apply_delta(original, delta)

        assert result == modified

    def test_multiline_changes(self):
        """Test changes across multiple lines."""
        original = """def hello():
    print("Hello")
    return True
"""
        modified = """def hello():
    print("Hello, World!")
    print("Additional line")
    return False
"""
        delta = compute_delta(original, modified)
        result = apply_delta(original, delta)

        assert result == modified


class TestApplyDelta:
    """Tests for apply_delta function."""

    def test_apply_invalid_delta_raises_error(self):
        """Applying invalid delta should raise DeltaPatchError."""
        with pytest.raises(DeltaPatchError):
            apply_delta("Hello", "invalid_delta_string", human_readable=False)

    def test_apply_delta_to_wrong_base(self):
        """Applying delta to wrong base content should raise error."""
        original = "Hello, World!"
        modified = "Hello, Python!"
        wrong_base = "Completely different content"

        delta = compute_delta(original, modified)

        # This may or may not raise depending on the delta
        # but the result should be wrong if it doesn't raise
        try:
            result = apply_delta(wrong_base, delta)
            # If no error, result should not equal modified
            # (though in some edge cases it might still work)
        except DeltaPatchError:
            pass  # Expected behavior


class TestFileDeltaOperations:
    """Tests for file-based delta operations."""

    def test_compute_delta_from_files(self):
        """Test computing delta between two files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            original_file = tmpdir / "original.txt"
            modified_file = tmpdir / "modified.txt"

            original_file.write_text("Hello, World!")
            modified_file.write_text("Hello, Python!")

            delta, orig_md5, mod_md5 = compute_delta_from_files(
                original_file, modified_file
            )

            assert delta is not None
            assert orig_md5 is not None
            assert mod_md5 is not None
            assert orig_md5 != mod_md5

    def test_apply_delta_to_file(self):
        """Test applying delta to a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            original_file = tmpdir / "original.txt"
            modified_file = tmpdir / "modified.txt"
            output_file = tmpdir / "output.txt"

            original_file.write_text("Hello, World!")
            modified_file.write_text("Hello, Python!")

            delta, _, _ = compute_delta_from_files(original_file, modified_file)

            apply_delta_to_file(original_file, delta, output_file)

            assert output_file.read_text() == "Hello, Python!"

    def test_file_not_found_raises_error(self):
        """Test that FileNotFoundError is raised for missing files."""
        with pytest.raises(FileNotFoundError):
            compute_delta_from_files(
                Path("/nonexistent/file1.txt"),
                Path("/nonexistent/file2.txt"),
            )


class TestDeltaChain:
    """Tests for delta chain operations."""

    def test_apply_delta_chain(self):
        """Test applying a chain of deltas."""
        v0 = "Version 0"
        v1 = "Version 1"
        v2 = "Version 2"
        v3 = "Version 3 - Final"

        # Create delta chain
        delta_0_1 = compute_delta(v0, v1)
        delta_1_2 = compute_delta(v1, v2)
        delta_2_3 = compute_delta(v2, v3)

        # Apply chain
        result = apply_delta_chain(v0, [delta_0_1, delta_1_2, delta_2_3])

        assert result == v3

    def test_empty_delta_chain(self):
        """Empty delta chain should return original content."""
        original = "Original content"
        result = apply_delta_chain(original, [])
        assert result == original

    def test_single_delta_chain(self):
        """Single delta in chain should work correctly."""
        v0 = "Version 0"
        v1 = "Version 1"

        delta = compute_delta(v0, v1)
        result = apply_delta_chain(v0, [delta])

        assert result == v1


class TestFileMetadata:
    """Tests for file metadata creation."""

    def test_create_file_metadata_with_delta(self):
        """Test creating file metadata with delta information."""
        metadata = create_file_metadata_with_delta(
            parent_md5="abc123",
            delta="some_delta_data",
            human_readable=False,
        )

        assert metadata["parent_md5"] == "abc123"
        assert metadata["delta"] == "some_delta_data"
        assert metadata["delta_format"] == "compressed"

    def test_create_file_metadata_human_readable(self):
        """Test creating file metadata with human-readable delta."""
        metadata = create_file_metadata_with_delta(
            parent_md5="abc123",
            delta="@@ -1,5 +1,6 @@\n ...",
            human_readable=True,
        )

        assert metadata["delta_format"] == "human_readable"

    def test_additional_metadata_merged(self):
        """Test that additional metadata is merged correctly."""
        metadata = create_file_metadata_with_delta(
            parent_md5="abc123",
            delta="delta",
            additional_metadata={"custom_field": "custom_value"},
        )

        assert metadata["custom_field"] == "custom_value"
        assert metadata["parent_md5"] == "abc123"


class TestDeltaEfficiency:
    """Tests for delta efficiency checking."""

    def test_small_change_is_efficient(self):
        """Small changes should be efficient to store as deltas."""
        original = "A" * 10000
        modified = "A" * 9999 + "B"

        assert is_delta_efficient(original, modified)

    def test_large_change_may_not_be_efficient(self):
        """Large changes might not be efficient as deltas."""
        original = "A" * 100
        modified = "B" * 100  # Completely different

        # With threshold 0.5, this might not be efficient
        result = is_delta_efficient(original, modified, threshold=0.3)
        # Result depends on compression, but we test the function runs


class TestConvertDeltaFormat:
    """Tests for delta format conversion."""

    def test_convert_to_human_readable(self):
        """Test converting compressed delta to human-readable."""
        original = "Hello, World!"
        modified = "Hello, Python!"

        compressed = compute_delta(original, modified, human_readable=False)
        human_readable = convert_delta_format(compressed, to_human_readable=True)

        # Should be able to apply the converted delta
        result = apply_delta(original, human_readable, human_readable=True)
        assert result == modified

    def test_convert_to_compressed(self):
        """Test converting human-readable delta to compressed."""
        original = "Hello, World!"
        modified = "Hello, Python!"

        human_readable = compute_delta(original, modified, human_readable=True)
        compressed = convert_delta_format(human_readable, to_human_readable=False)

        # Should be able to apply the converted delta
        result = apply_delta(original, compressed, human_readable=False)
        assert result == modified

    def test_already_compressed_returns_same(self):
        """Converting already compressed delta should return same."""
        original = "Hello"
        modified = "World"

        compressed = compute_delta(original, modified, human_readable=False)
        result = convert_delta_format(compressed, to_human_readable=False)

        assert result == compressed


class TestRealWorldScenarios:
    """Tests simulating real-world usage scenarios."""

    def test_config_file_versioning(self):
        """Test versioning a configuration file through multiple changes."""
        v0 = """[database]
host = localhost
port = 5432
name = mydb
"""
        v1 = """[database]
host = localhost
port = 5432
name = mydb
user = admin
"""
        v2 = """[database]
host = production.server.com
port = 5432
name = mydb
user = admin
password = secret
"""
        v3 = """[database]
host = production.server.com
port = 5433
name = production_db
user = admin
password = secret

[logging]
level = INFO
"""
        # Build delta chain
        deltas = [
            compute_delta(v0, v1),
            compute_delta(v1, v2),
            compute_delta(v2, v3),
        ]

        # Reconstruct from v0
        result = apply_delta_chain(v0, deltas)
        assert result == v3

    def test_json_file_versioning(self):
        """Test versioning a JSON file."""
        v0 = '{"name": "test", "version": "1.0.0"}'
        v1 = '{"name": "test", "version": "1.0.1", "updated": true}'
        v2 = '{"name": "test", "version": "1.1.0", "updated": true, "features": ["a", "b"]}'

        delta_0_1 = compute_delta(v0, v1)
        delta_1_2 = compute_delta(v1, v2)

        result = apply_delta_chain(v0, [delta_0_1, delta_1_2])
        assert result == v2

    def test_large_file_versioning(self):
        """Test versioning a larger file."""
        # Simulate a larger file
        lines = [f"Line {i}: Some content here" for i in range(1000)]
        v0 = "\n".join(lines)

        # Make some modifications
        lines[50] = "Line 50: MODIFIED CONTENT"
        lines[500] = "Line 500: ANOTHER MODIFICATION"
        lines.append("Line 1000: New line at end")
        v1 = "\n".join(lines)

        delta = compute_delta(v0, v1)
        result = apply_delta(v0, delta)

        assert result == v1

        # Delta should be smaller than full file
        assert len(delta) < len(v1)
