"""
File class for representing SQL files with Jinja templating support.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from jinja2 import Environment, FileSystemLoader, Template, select_autoescape


class File:
    """
    Represents a SQL file with optional Jinja templating.

    Examples:
        # Simple SQL file
        File("queries/user_analysis.sql")

        # SQL file with Jinja variables
        File("queries/date_range_report.sql", variables={"start_date": "2025-01-01"})

        # SQL file with custom base path
        File("user_analysis.sql", base_path="/path/to/sql/files")
    """

    def __init__(
        self,
        file_path: str,
        variables: Optional[Dict[str, Any]] = None,
        base_path: Optional[str] = None,
    ):
        self.file_path = file_path
        self.variables = variables or {}
        self.base_path = base_path
        self._content: Optional[str] = None

    @property
    def full_path(self) -> Path:
        """
        Get the full path to the SQL file.

        Uses the base_path if provided, otherwise falls back to default 'sql' directory.
        """
        if Path(self.file_path).is_absolute():
            return Path(self.file_path)

        if self.base_path:
            return Path(self.base_path) / self.file_path

        return Path('sql') / self.file_path

    def read_content(self) -> str:
        """Read the SQL file content."""
        if self._content is None:
            if not self.full_path.exists():
                raise FileNotFoundError(f'SQL file not found: {self.full_path}')

            self._content = self.full_path.read_text(encoding='utf-8')

        return self._content

    def render(self, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Render the SQL file with Jinja templating.

        Args:
            context: Additional context variables for Jinja rendering

        Returns:
            Rendered SQL string
        """
        if Path(self.file_path).is_absolute():
            sql_path = Path(self.file_path)
            if not sql_path.exists():
                raise FileNotFoundError(f'SQL file not found: {sql_path}')
            sql_content = sql_path.read_text(encoding='utf-8')
        else:
            try:
                search_paths = ['dags/git_sql', 'sql/', 'dags/sql']

                env = Environment(
                    loader=FileSystemLoader(search_paths),
                    autoescape=select_autoescape(['html', 'xml']),
                )

                template = env.get_template(self.file_path)
                sql_content = template.source

            except Exception as e:
                search_paths_str = ', '.join(search_paths)
                raise FileNotFoundError(
                    f'SQL file not found: {self.file_path}. '
                    f'Searched in: {search_paths_str}'
                ) from e

        if not self.variables and not context:
            return sql_content

        template_vars = {**self.variables}
        if context:
            template_vars.update(context)

        template = Template(sql_content)
        return template.render(**template_vars)

    def __str__(self) -> str:
        """Return the rendered SQL content."""
        return self.render()

    def __repr__(self) -> str:
        return f"File(file_path='{self.file_path}')"
