# AST-Based Code Analysis Tools

This directory contains Python scripts that use the **AST (Abstract Syntax Tree)** module for static code analysis.

## What is AST?

**AST** (Abstract Syntax Tree) is Python's built-in module for parsing and analyzing Python code **without executing it**. It converts Python source code into a tree structure that represents the code's syntax and structure.

### Why Use AST?

✅ **Safety**: Analyze code without running it (no side effects, no security risks)  
✅ **Speed**: Fast parsing and analysis  
✅ **Completeness**: Access to all code elements (functions, classes, imports, type hints, docstrings)  
✅ **Power**: Build custom code analysis and transformation tools  

### Common Use Cases

- **Code Quality Tools**: pylint, flake8, mypy
- **Code Formatters**: black, autopep8
- **Documentation Generators**: sphinx, pdoc
- **Import Analyzers**: isort
- **Static Type Checkers**: mypy, pyright
- **Custom Code Auditing**: Security scanners, complexity analyzers

## Scripts in This Directory

### 1. `check_docstring_coverage.py`

Analyzes docstring coverage in Python files to identify undocumented functions.

**Usage:**
```powershell
python scripts/tools/check_docstring_coverage.py <python_file>
```

**Example:**
```powershell
python scripts/tools/check_docstring_coverage.py scripts/tools/compare_hosts.py
```

**Output:**
- Total function count
- Documented vs undocumented functions
- List of functions with/without docstrings
- Module-level docstring check
- Quality assessment (Excellent/Good/Fair/Poor/Critical)

**What it does:**
1. Parses the Python file using `ast.parse()`
2. Walks the AST to find all `FunctionDef` nodes
3. Checks each function for docstrings using `ast.get_docstring()`
4. Generates a coverage report

### 2. `ast_examples.py`

Interactive examples demonstrating AST capabilities.

**Usage:**
```powershell
python scripts/tools/ast_examples.py
```

**Examples Included:**
1. **Basic Parsing**: Parse code and inspect AST structure
2. **Extract Functions**: Find all function definitions with signatures
3. **Analyze Imports**: Extract import statements
4. **Type Hints**: Extract function type annotations
5. **Docstring Coverage**: Calculate documentation coverage
6. **Code Transformation**: Modify code using AST visitors

## Quick Reference: Key AST Functions

```python
import ast

# Parse Python code into an AST
tree = ast.parse(code_string)

# Walk through all nodes in the tree
for node in ast.walk(tree):
    print(type(node))

# Extract docstrings
docstring = ast.get_docstring(function_node)

# Convert AST back to Python code
code = ast.unparse(tree)

# Common node types
ast.FunctionDef      # Regular function
ast.AsyncFunctionDef # Async function
ast.ClassDef         # Class definition
ast.Import           # import statement
ast.ImportFrom       # from ... import statement
ast.Call             # Function call
ast.Assign           # Assignment (x = 1)
```

## How AST Was Used in This Project

During the documentation improvement for `compare_hosts.py`, AST was used to:

1. **Identify undocumented functions**: Count functions without docstrings
2. **Track progress**: Calculate coverage percentage (3.6% → 32.1%)
3. **Generate reports**: List which functions need documentation
4. **Quality control**: Ensure public API is properly documented

**Before:**
```
Total functions: 28
Functions with docstrings: 1 (3.6%)
```

**After:**
```
Total functions: 28
Functions with docstrings: 9 (32.1%)
```

## Real-World AST Tools

Popular tools built on AST:

| Tool | Purpose |
|------|---------|
| **pylint** | Code quality checker |
| **black** | Code formatter |
| **mypy** | Static type checker |
| **isort** | Import statement organizer |
| **flake8** | Style guide enforcer |
| **bandit** | Security issue finder |
| **radon** | Code complexity analyzer |

## Learn More

- [Python AST Documentation](https://docs.python.org/3/library/ast.html)
- [Green Tree Snakes - AST Tutorial](https://greentreesnakes.readthedocs.io/)
- [ast.parse() Examples](https://docs.python.org/3/library/ast.html#ast.parse)

## Benefits of Static Analysis (AST) vs Dynamic Analysis

| AST (Static) | Import/Exec (Dynamic) |
|--------------|----------------------|
| ✅ Never executes code | ❌ Runs code |
| ✅ Fast | ❌ Slower |
| ✅ Safe (no side effects) | ❌ Can have side effects |
| ✅ Works on broken code | ❌ Requires valid code |
| ✅ Language-aware | ❌ Runtime-dependent |

---

**Pro Tip**: Always prefer AST for code analysis unless you specifically need runtime information. It's safer, faster, and more reliable.
