"""Operation summary tracking for airsql operators."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class OperationSummary:
    """Tracks metrics and summary information for data operations."""

    operation_type: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    rows_skipped: int = 0
    duration_seconds: float = 0.0
    file_size_mb: Optional[float] = None
    format_used: str = 'parquet'
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    dry_run: bool = False

    def to_log_summary(self) -> str:
        """Generate a formatted summary string for logging.

        Returns:
            Formatted summary string with all operation metrics.
        """
        lines = []

        # Header
        dry_run_prefix = '[DRY RUN] ' if self.dry_run else ''
        lines.append(f'{dry_run_prefix}Operation: {self.operation_type}')

        # Core metrics
        lines.append(f'  Rows extracted: {self.rows_extracted}')
        lines.append(f'  Rows loaded: {self.rows_loaded}')
        if self.rows_skipped > 0:
            lines.append(f'  Rows skipped: {self.rows_skipped}')

        # Timing and format
        lines.append(f'  Duration: {self.duration_seconds:.2f}s')
        lines.append(f'  Format: {self.format_used}')

        # File size if available
        if self.file_size_mb is not None:
            lines.append(f'  File size: {self.file_size_mb:.2f} MB')

        # Validation results
        if self.validation_errors:
            lines.append('  Validation errors:')
            for error in self.validation_errors:
                lines.append(f'    - {error}')

        if self.validation_warnings:
            lines.append('  Validation warnings:')
            for warning in self.validation_warnings:
                lines.append(f'    - {warning}')

        # Dry run notice
        if self.dry_run:
            lines.append('  [DRY RUN] No actual data written')

        return '\n'.join(lines)

    def to_dict(self) -> dict:
        """Convert summary to dictionary for structured logging."""
        return {
            'operation_type': self.operation_type,
            'rows_extracted': self.rows_extracted,
            'rows_loaded': self.rows_loaded,
            'rows_skipped': self.rows_skipped,
            'duration_seconds': round(self.duration_seconds, 2),
            'file_size_mb': round(self.file_size_mb, 2) if self.file_size_mb else None,
            'format_used': self.format_used,
            'validation_errors': self.validation_errors,
            'validation_warnings': self.validation_warnings,
            'dry_run': self.dry_run,
        }
