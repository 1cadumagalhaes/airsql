import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from airsql.transfers.gcs_postgres import GCSToPostgresOperator


class TestDetectJsonColumns:
    def test_detect_json_columns_returns_json_columns(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('metadata',), ('config',)]

        result = GCSToPostgresOperator._detect_json_columns(
            mock_hook, 'public', 'test_table'
        )

        assert result == {'metadata', 'config'}

    def test_detect_json_columns_returns_empty_set_when_no_json_columns(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []

        result = GCSToPostgresOperator._detect_json_columns(
            mock_hook, 'public', 'test_table'
        )

        assert result == set()

    def test_detect_json_columns_handles_exception(self):
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = Exception('DB error')

        result = GCSToPostgresOperator._detect_json_columns(
            mock_hook, 'public', 'test_table'
        )

        assert result == set()


class TestFixJsonQuoting:
    def test_fix_json_quoting_converts_single_quotes_to_double(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'metadata': ["{'key': 'value'}", "{'name': 'test', 'count': 1}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['metadata'][0] == '{"key": "value"}'
        assert result['metadata'][1] == '{"name": "test", "count": 1}'

    def test_fix_json_quoting_handles_nested_structures(self):
        df = pd.DataFrame({
            'id': [1],
            'config': ["{'nested': {'deep': 'value'}}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'config'})

        assert result['config'][0] == '{"nested": {"deep": "value"}}'

    def test_fix_json_quoting_handles_lists(self):
        df = pd.DataFrame({
            'id': [1],
            'items': ["['a', 'b', 'c']"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'items'})

        assert result['items'][0] == '["a", "b", "c"]'

    def test_fix_json_quoting_handles_null_values(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'metadata': [None, "{'key': 'value'}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert pd.isna(result['metadata'][0])
        assert result['metadata'][1] == '{"key": "value"}'

    def test_fix_json_quoting_handles_nan_values(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'metadata': [float('nan'), "{'key': 'value'}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert pd.isna(result['metadata'][0])
        assert result['metadata'][1] == '{"key": "value"}'

    def test_fix_json_quoting_leaves_valid_json_unchanged(self):
        df = pd.DataFrame({
            'id': [1],
            'metadata': ['{"already": "valid"}'],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['metadata'][0] == '{"already": "valid"}'

    def test_fix_json_quoting_handles_mixed_types(self):
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'metadata': [
                "{'key': 'value'}",
                'plain string',
                '123',
            ],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['metadata'][0] == '{"key": "value"}'
        assert result['metadata'][1] == 'plain string'
        assert result['metadata'][2] == '123'

    def test_fix_json_quoting_ignores_non_json_columns(self):
        df = pd.DataFrame({
            'id': [1],
            'name': ["{'this': 'looks like json'}"],
            'metadata': ["{'actual': 'json'}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['name'][0] == "{'this': 'looks like json'}"
        assert result['metadata'][0] == '{"actual": "json"}'

    def test_fix_json_quoting_handles_missing_column(self):
        df = pd.DataFrame({
            'id': [1],
            'name': ['test'],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'nonexistent'})

        assert 'name' in result.columns
        assert result['name'][0] == 'test'

    def test_fix_json_quoting_handles_complex_nested_json(self):
        df = pd.DataFrame({
            'id': [1],
            'data': ["{'outer': {'inner': ['a', 'b']}, 'nums': [1, 2, 3]}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'data'})

        parsed = json.loads(result['data'][0])
        assert parsed == {'outer': {'inner': ['a', 'b']}, 'nums': [1, 2, 3]}
