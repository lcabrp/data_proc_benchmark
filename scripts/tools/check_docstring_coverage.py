"""Check docstring coverage in Python files.

AST (Abstract Syntax Tree) is a powerful Python module that parses Python source code
into a tree representation. It allows you to analyze code structure without executing it.

What is AST used for?
---------------------
1. **Code Analysis**: Inspect functions, classes, imports without running the code
2. **Static Analysis Tools**: Build linters, formatters (like pylint, black, mypy)
3. **Code Generation**: Transform or generate Python code programmatically
4. **Documentation Tools**: Extract docstrings, type hints, function signatures
5. **Metaprogramming**: Modify code structure at runtime or build time
6. **Security Auditing**: Detect dangerous patterns without execution

In this script, we use AST to:
- Parse Python source files into a tree structure
- Walk through all nodes (functions, classes, etc.)
- Extract docstrings from function definitions
- Calculate documentation coverage without importing/running the code

This is safer and faster than importing modules, which could have side effects.
"""

import ast
import sys
from pathlib import Path
from typing import List, Tuple


def analyze_docstring_coverage(file_path: str) -> Tuple[int, int, List[str], List[str]]:
    """Analyze docstring coverage in a Python file using AST.
    
    Args:
        file_path: Path to Python file to analyze
        
    Returns:
        Tuple of (total_functions, documented_functions, documented_names, undocumented_names)
    """
    # Read the source code
    with open(file_path, encoding='utf-8') as f:
        code = f.read()
    
    # Parse the code into an Abstract Syntax Tree
    # This converts Python code into a tree of nodes representing the code structure
    tree = ast.parse(code)
    
    # Find all function definitions in the tree
    # ast.walk() recursively visits all nodes in the tree
    # We filter for FunctionDef nodes (regular functions) and AsyncFunctionDef (async functions)
    funcs = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    
    # Check which functions have docstrings
    # ast.get_docstring() extracts the docstring from a function node
    funcs_with_docs = [f for f in funcs if ast.get_docstring(f)]
    funcs_without_docs = [f for f in funcs if not ast.get_docstring(f)]
    
    # Get function names for reporting
    documented_names = [f.name for f in funcs_with_docs]
    undocumented_names = [f.name for f in funcs_without_docs]
    
    return len(funcs), len(funcs_with_docs), documented_names, undocumented_names


def print_coverage_report(file_path: str) -> None:
    """Print a detailed docstring coverage report for a Python file.
    
    Args:
        file_path: Path to Python file to analyze
    """
    try:
        total, documented, doc_names, undoc_names = analyze_docstring_coverage(file_path)
        
        print(f"\nDocstring Coverage Analysis: {file_path}")
        print("=" * 70)
        print(f"Total functions: {total}")
        print(f"Functions with docstrings: {documented} ({documented/total*100:.1f}%)" if total > 0 else "No functions found")
        print(f"Functions without docstrings: {len(undoc_names)} ({len(undoc_names)/total*100:.1f}%)" if total > 0 else "")
        
        if doc_names:
            print(f"\n✓ Documented functions ({len(doc_names)}):")
            for name in sorted(doc_names):
                print(f"  ✓ {name}")
        
        if undoc_names:
            print(f"\n✗ Missing docstrings ({len(undoc_names)}):")
            for name in sorted(undoc_names):
                print(f"  ✗ {name}")
        
        # Check for module-level docstring
        with open(file_path, encoding='utf-8') as f:
            code = f.read()
        tree = ast.parse(code)
        module_doc = ast.get_docstring(tree)
        
        print(f"\nModule-level docstring: {'✓ Present' if module_doc else '✗ Missing'}")
        
        # Provide quality assessment
        print("\n" + "=" * 70)
        coverage_pct = (documented / total * 100) if total > 0 else 0
        if coverage_pct >= 80:
            print("Assessment: ✓ EXCELLENT - Well documented code")
        elif coverage_pct >= 60:
            print("Assessment: ✓ GOOD - Decent documentation coverage")
        elif coverage_pct >= 40:
            print("Assessment: ⚠ FAIR - Consider adding more documentation")
        elif coverage_pct >= 20:
            print("Assessment: ⚠ POOR - Needs significant documentation improvement")
        else:
            print("Assessment: ✗ CRITICAL - Severely lacking documentation")
        
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    except SyntaxError as e:
        print(f"Error: Invalid Python syntax in {file_path}")
        print(f"  {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error analyzing {file_path}: {e}")
        sys.exit(1)


def main():
    """Main entry point for the docstring coverage checker."""
    if len(sys.argv) < 2:
        print("Usage: python check_docstring_coverage.py <python_file>")
        print("\nExample:")
        print("  python check_docstring_coverage.py scripts/tools/compare_hosts.py")
        print("\nWhat this script does:")
        print("  Uses Python's AST (Abstract Syntax Tree) module to analyze")
        print("  docstring coverage in Python files without executing the code.")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Convert to Path for better path handling
    path = Path(file_path)
    if not path.exists():
        print(f"Error: File does not exist: {file_path}")
        sys.exit(1)
    
    if path.suffix != '.py':
        print(f"Warning: File does not have .py extension: {file_path}")
        print("Proceeding anyway...\n")
    
    print_coverage_report(str(path))


if __name__ == '__main__':
    main()
