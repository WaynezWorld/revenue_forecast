"""
Unit test to prevent unconditional DELETE statements on forecast/backtest tables.

This test scans the repository for dangerous DELETE statements in:
- Jupyter notebooks (.ipynb files)
- SQL files (.sql)

FAIL conditions:
- DELETE FROM FORECAST_MODEL_BACKTEST_PREDICTIONS without WHERE clause in same cell/file
- DELETE FROM FORECAST_OUTPUT_* without WHERE clause

This is a static safeguard added as part of hotfix/backtest-delete-safeguard PR.
"""

import json
import re
from pathlib import Path


def test_no_unconditional_deletes_in_notebooks():
    """
    Check all .ipynb files for unconditional DELETE statements.
    
    A DELETE is considered unconditional if:
    - The cell contains "DELETE FROM <forecast_table>"
    - The cell does NOT contain "WHERE" (case-insensitive)
    """
    repo_root = Path(__file__).parent.parent
    notebooks = list(repo_root.glob("**/*.ipynb"))
    
    assert len(notebooks) > 0, "No notebooks found - test may be misconfigured"
    
    dangerous_patterns = [
        r"delete\s+from\s+.*forecast_model_backtest_predictions",
        r"delete\s+from\s+.*forecast_output_pc_reason",
        r"delete\s+from\s+.*forecast_model_predictions",
        r"delete\s+from\s+.*forecast_output_.*",
    ]
    
    violations = []
    
    for nb_path in notebooks:
        try:
            with open(nb_path, 'r', encoding='utf-8') as f:
                nb = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Skip malformed notebooks
            print(f"Warning: Could not parse {nb_path}: {e}")
            continue
        
        for cell_idx, cell in enumerate(nb.get('cells', [])):
            source = ''.join(cell.get('source', []))
            source_lower = source.lower()
            
            # Check if cell contains DELETE on forecast tables
            has_delete = any(re.search(pat, source_lower) for pat in dangerous_patterns)
            
            if has_delete:
                # Check if WHERE clause is present
                has_where = 'where' in source_lower
                
                if not has_where:
                    violations.append({
                        'file': nb_path.relative_to(repo_root),
                        'cell': cell_idx + 1,
                        'snippet': source[:200] + ('...' if len(source) > 200 else '')
                    })
    
    if violations:
        error_msg = "\n❌ UNCONDITIONAL DELETE DETECTED - SAFETY VIOLATION\n\n"
        for v in violations:
            error_msg += f"File: {v['file']}\n"
            error_msg += f"Cell: {v['cell']}\n"
            error_msg += f"Snippet:\n{v['snippet']}\n"
            error_msg += "-" * 80 + "\n"
        error_msg += "\n⚠️  All DELETE statements on forecast tables MUST include WHERE clause.\n"
        error_msg += "⚠️  Use timestamped backup tables before deletion (see sql/hotfixes/safe_backtest_delete.sql).\n"
        
        assert False, error_msg
    
    print(f"✅ Scanned {len(notebooks)} notebooks - no unconditional DELETEs found")


def test_no_unconditional_deletes_in_sql_files():
    """
    Check all .sql files for unconditional DELETE statements.
    
    Exceptions:
    - Comments (-- or /* */)
    - Template files with :placeholders
    """
    repo_root = Path(__file__).parent.parent
    sql_files = list(repo_root.glob("**/*.sql"))
    
    if len(sql_files) == 0:
        print("No SQL files found - skipping SQL check")
        return
    
    dangerous_patterns = [
        r"delete\s+from\s+.*forecast_model_backtest_predictions",
        r"delete\s+from\s+.*forecast_output_pc_reason",
        r"delete\s+from\s+.*forecast_model_predictions",
        r"delete\s+from\s+.*forecast_output_.*",
    ]
    
    violations = []
    
    for sql_path in sql_files:
        try:
            with open(sql_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            print(f"Warning: Could not read {sql_path} as UTF-8")
            continue
        
        content_lower = content.lower()
        
        # Remove SQL comments for analysis
        # Remove single-line comments
        content_no_comments = re.sub(r'--.*$', '', content_lower, flags=re.MULTILINE)
        # Remove multi-line comments
        content_no_comments = re.sub(r'/\*.*?\*/', '', content_no_comments, flags=re.DOTALL)
        
        # Check for DELETE statements
        has_delete = any(re.search(pat, content_no_comments) for pat in dangerous_patterns)
        
        if has_delete:
            # Extract DELETE statement
            delete_matches = re.finditer(
                r"(delete\s+from\s+[^\s;]+.*?)(?:;|$)",
                content_no_comments,
                re.IGNORECASE | re.DOTALL
            )
            
            for match in delete_matches:
                stmt = match.group(1)
                
                # Check if this DELETE is on a forecast table
                is_forecast_delete = any(re.search(pat, stmt) for pat in dangerous_patterns)
                
                if is_forecast_delete:
                    # Check if WHERE is present
                    has_where = 'where' in stmt
                    
                    # Allow template placeholders (e.g., :TARGET_MODEL_RUN_IDS)
                    has_placeholder = ':' in stmt or '{' in stmt
                    
                    if not has_where and not has_placeholder:
                        violations.append({
                            'file': sql_path.relative_to(repo_root),
                            'snippet': stmt[:300] + ('...' if len(stmt) > 300 else '')
                        })
    
    if violations:
        error_msg = "\n❌ UNCONDITIONAL DELETE DETECTED IN SQL - SAFETY VIOLATION\n\n"
        for v in violations:
            error_msg += f"File: {v['file']}\n"
            error_msg += f"Snippet:\n{v['snippet']}\n"
            error_msg += "-" * 80 + "\n"
        error_msg += "\n⚠️  All DELETE statements on forecast tables MUST include WHERE clause.\n"
        
        assert False, error_msg
    
    print(f"✅ Scanned {len(sql_files)} SQL files - no unconditional DELETEs found")


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
