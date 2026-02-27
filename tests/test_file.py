import tempfile
from pathlib import Path

import pytest

from airsql.file import File


class TestFile:
    def test_render_with_template_source_fix(self) -> None:
        """Test that File.render() works correctly without using template.source."""
        # Create a temporary SQL file with Jinja templating
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('SELECT * FROM {{ table_name }} WHERE id = {{ id }}')
            temp_file_path = f.name

        try:
            # Create File object with absolute path
            file_obj = File(temp_file_path)

            # Render with context
            rendered = file_obj.render({'table_name': 'users', 'id': 123})

            # Should render correctly
            assert rendered == 'SELECT * FROM users WHERE id = 123'
        finally:
            # Cleanup
            Path(temp_file_path).unlink()

    def test_render_without_context(self) -> None:
        """Test that File.render() works without context variables."""
        # Create a temporary SQL file without Jinja templating
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('SELECT * FROM users WHERE id = 123')
            temp_file_path = f.name

        try:
            # Create File object with absolute path
            file_obj = File(temp_file_path)

            # Render without context
            rendered = file_obj.render()

            # Should return the original content
            assert rendered == 'SELECT * FROM users WHERE id = 123'
        finally:
            # Cleanup
            Path(temp_file_path).unlink()

    def test_render_with_variables(self) -> None:
        """Test that File.render() works with variables provided at initialization."""
        # Create a temporary SQL file with Jinja templating
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write('SELECT * FROM {{ table_name }} WHERE id = {{ id }}')
            temp_file_path = f.name

        try:
            # Create File object with absolute path and variables
            file_obj = File(temp_file_path, variables={'id': 456})

            # Render with additional context
            rendered = file_obj.render({'table_name': 'orders'})

            # Should render correctly combining variables and context
            assert rendered == 'SELECT * FROM orders WHERE id = 456'
        finally:
            # Cleanup
            Path(temp_file_path).unlink()

    def test_file_not_found(self) -> None:
        """Test that File.render() raises FileNotFoundError for non-existent files."""
        file_obj = File('nonexistent.sql')

        with pytest.raises(FileNotFoundError):
            file_obj.render()
