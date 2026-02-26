"""Tests for markdown response formatters."""
from __future__ import annotations

from ontario_data.formatting import format_records, format_search_results, md_response, md_table


class TestMdTable:
    def test_basic_table(self):
        result = md_table(["Name", "Age"], [["Alice", 30], ["Bob", 25]])
        lines = result.strip().split("\n")
        assert lines[0] == "| Name | Age |"
        assert lines[1] == "| --- | --- |"
        assert "| Alice | 30 |" in result
        assert "| Bob | 25 |" in result

    def test_empty_rows(self):
        result = md_table(["Name"], [])
        assert "| Name |" in result
        assert len(result.strip().split("\n")) == 2  # header + separator only

    def test_empty_headers(self):
        assert md_table([], []) == ""

    def test_none_values(self):
        result = md_table(["A", "B"], [["x", None]])
        assert "| x |  |" in result

    def test_pipe_in_value_escaped(self):
        result = md_table(["Name"], [["Cat | Dog"]])
        assert "Cat \\| Dog" in result
        lines = result.strip().split("\n")
        assert len(lines) == 3  # header, sep, one data row

    def test_newline_in_value_replaced(self):
        result = md_table(["Text"], [["line1\nline2"]])
        data_line = result.strip().split("\n")[2]
        assert "line1 line2" in data_line

    def test_all_values_stringified(self):
        """All types are str()-ed, no comma formatting."""
        result = md_table(["Count"], [[1373]])
        assert "| 1373 |" in result  # NOT "1,373"

    def test_row_shorter_than_headers(self):
        """Missing columns filled with empty string."""
        result = md_table(["A", "B", "C"], [["x"]])
        lines = result.strip().split("\n")
        assert lines[2].count("|") == 4  # | x |  |  |

    def test_row_longer_than_headers(self):
        """Extra columns are silently dropped."""
        result = md_table(["A", "B"], [["x", "y", "z"]])
        lines = result.strip().split("\n")
        assert "| x | y |" == lines[2]


class TestMdResponse:
    def test_scalar_values(self):
        result = md_response(status="ok", count=42)
        assert "**status:** ok" in result
        assert "**count:** 42" in result

    def test_list_of_dicts_becomes_table(self):
        result = md_response(
            total=2,
            records=[
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ],
        )
        assert "**total:** 2" in result
        assert "| name | age |" in result
        assert "| Alice | 30 |" in result

    def test_plain_list_becomes_comma_separated(self):
        result = md_response(tags=["water", "health", "environment"])
        assert "water, health, environment" in result

    def test_nested_dict_becomes_bullets(self):
        result = md_response(staleness={"is_stale": False, "days": 3})
        assert "is_stale" in result
        assert "3" in result

    def test_empty_kwargs(self):
        result = md_response()
        assert result == ""

    def test_none_values_shown(self):
        result = md_response(maintainer=None)
        assert "**maintainer:** None" in result

    def test_preserves_original_number_format(self):
        """Numbers should NOT get comma formatting — preserves copy-paste to SQL."""
        result = md_response(count=1373)
        assert "1373" in result

    def test_list_of_dicts_empty_list(self):
        result = md_response(records=[])
        assert "**records:** (none)" in result

    def test_handles_datetime_as_str(self):
        """Non-serializable types get str()-ed."""
        from datetime import datetime, timezone

        result = md_response(ts=datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert "2024" in result

    def test_keys_preserved_verbatim(self):
        """Keys are NOT transformed (no underscore-to-space)."""
        result = md_response(row_count=10)
        assert "**row_count:**" in result

    def test_heterogeneous_list_not_treated_as_table(self):
        """A list with mixed types should not attempt table rendering."""
        result = md_response(items=[{"a": 1}, "stray"])
        # Should not crash; falls through to plain list
        assert "items" in result


class TestFormatRecords:
    def test_basic(self):
        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        result = format_records(records, row_count=2)
        assert "**2 rows**" in result
        assert "| name | age |" in result
        assert "| Alice | 30 |" in result

    def test_with_source(self):
        records = [{"x": 1}]
        result = format_records(records, row_count=1, source="ds_my_table")
        assert "ds_my_table" in result

    def test_with_total(self):
        records = [{"x": 1}]
        result = format_records(records, row_count=1, total=5000)
        assert "5000 total" in result

    def test_preview_mode(self):
        records = [{"x": 1}]
        result = format_records(records, row_count=1, total=50000, preview=True)
        assert "Previewing" in result

    def test_empty(self):
        result = format_records([], row_count=0)
        assert "**0 rows**" in result

    def test_preserves_all_data(self):
        """All rows and columns must be present — never truncate."""
        records = [{"a": i, "b": i * 2, "c": i * 3} for i in range(100)]
        result = format_records(records, row_count=100)
        assert "| 0 | 0 | 0 |" in result
        assert "| 99 | 198 | 297 |" in result

    def test_with_fields_metadata(self):
        """Field type info is rendered as a schema line."""
        records = [{"Year": 2020, "Name": "Lead"}]
        fields = [{"name": "Year", "type": "numeric"}, {"name": "Name", "type": "text"}]
        result = format_records(records, row_count=1, fields=fields)
        assert "Year (numeric)" in result
        assert "Name (text)" in result


class TestFormatSearchResults:
    def test_with_results(self):
        results = [
            {
                "portal": "toronto",
                "portal_name": "Toronto Open Data",
                "total_count": 5,
                "returned": 2,
                "datasets": [
                    {
                        "id": "toronto:animal-intake",
                        "title": "Animal Services Intake",
                        "organization": "Municipal Licensing",
                        "formats": ["CSV", "XLSX"],
                        "description": "Animals taken in",
                        "num_resources": 3,
                        "last_modified": "2024-01-15",
                        "update_frequency": "monthly",
                    },
                ],
            }
        ]
        result = format_search_results(
            query="animal shelter", portals_searched=3, results=results, skipped=[]
        )
        assert "animal shelter" in result
        assert "**Animal Services Intake**" in result
        assert "toronto:animal-intake" in result
        assert "CSV" in result

    def test_with_skipped_portal(self):
        result = format_search_results(
            query="test",
            portals_searched=1,
            results=[],
            skipped=[{"portal": "ottawa", "portal_name": "Ottawa", "reason": "timeout"}],
        )
        assert "timeout" in result

    def test_no_results(self):
        result = format_search_results(
            query="zzz", portals_searched=3, results=[], skipped=[]
        )
        assert "0" in result or "No results" in result

    def test_shows_available_vs_returned(self):
        results = [
            {
                "portal": "ontario",
                "portal_name": "Ontario Open Data",
                "total_count": 500,
                "returned": 5,
                "datasets": [{"id": f"ontario:ds{i}", "title": f"Dataset {i}", "organization": "", "formats": ["CSV"]} for i in range(5)],
            }
        ]
        result = format_search_results(query="water", portals_searched=1, results=results, skipped=[])
        assert "500 available" in result
