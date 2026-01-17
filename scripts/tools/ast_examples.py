"""Examples demonstrating how Python's AST (Abstract Syntax Tree) module works.

AST is used to parse and analyze Python code structure without executing it.
This is extremely useful for code analysis, linting, documentation generation,
and metaprogramming tasks.
"""

import ast
import inspect


def example_1_basic_parsing():
    """Example 1: Parse simple Python code and inspect its structure."""
    print("\n" + "="*70)
    print("EXAMPLE 1: Basic AST Parsing")
    print("="*70)
    
    # Simple Python code as a string
    code = """
def greet(name):
    '''Say hello to someone.'''
    return f"Hello, {name}!"
    
x = 42
y = x + 10
"""
    
    # Parse the code into an AST
    tree = ast.parse(code)
    
    # Display the AST structure
    print("\nAST Structure:")
    print(ast.dump(tree, indent=2))
    
    # Walk through all nodes
    print("\nAll node types in the code:")
    for node in ast.walk(tree):
        print(f"  - {node.__class__.__name__}")


def example_2_extract_functions():
    """Example 2: Extract all function definitions from code."""
    print("\n" + "="*70)
    print("EXAMPLE 2: Extract Function Definitions")
    print("="*70)
    
    code = """
def add(a, b):
    return a + b

def multiply(x, y):
    '''Multiply two numbers.'''
    return x * y

class Calculator:
    def divide(self, a, b):
        '''Divide a by b.'''
        return a / b
"""
    
    tree = ast.parse(code)
    
    # Find all function definitions
    functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    
    print("\nFound functions:")
    for func in functions:
        docstring = ast.get_docstring(func)
        args = [arg.arg for arg in func.args.args]
        print(f"\n  Function: {func.name}")
        print(f"    Arguments: {', '.join(args)}")
        print(f"    Docstring: {docstring if docstring else 'None'}")
        print(f"    Line number: {func.lineno}")


def example_3_analyze_imports():
    """Example 3: Extract all import statements from code."""
    print("\n" + "="*70)
    print("EXAMPLE 3: Analyze Import Statements")
    print("="*70)
    
    code = """
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional
import numpy as np
from pandas import DataFrame, Series
"""
    
    tree = ast.parse(code)
    
    print("\nImport analysis:")
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                asname = alias.asname
                print(f"  import {name}" + (f" as {asname}" if asname else ""))
        
        elif isinstance(node, ast.ImportFrom):
            module = node.module
            for alias in node.names:
                name = alias.name
                asname = alias.asname
                print(f"  from {module} import {name}" + (f" as {asname}" if asname else ""))


def example_4_type_hints():
    """Example 4: Extract type hints from function definitions."""
    print("\n" + "="*70)
    print("EXAMPLE 4: Extract Type Hints")
    print("="*70)
    
    code = """
def process_data(items: List[str], count: int = 10) -> Dict[str, int]:
    '''Process a list of items.'''
    return {item: count for item in items}

def calculate(x: float, y: float) -> float:
    return x + y
"""
    
    tree = ast.parse(code)
    
    print("\nFunction signatures with type hints:")
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            print(f"\n  {node.name}(")
            
            # Extract argument types
            for arg in node.args.args:
                arg_name = arg.arg
                arg_type = ast.unparse(arg.annotation) if arg.annotation else "Any"
                print(f"    {arg_name}: {arg_type}")
            
            # Extract return type
            return_type = ast.unparse(node.returns) if node.returns else "Any"
            print(f"  ) -> {return_type}")


def example_5_docstring_coverage():
    """Example 5: Calculate docstring coverage (like our script)."""
    print("\n" + "="*70)
    print("EXAMPLE 5: Docstring Coverage Analysis")
    print("="*70)
    
    code = """
def documented_function():
    '''This function has a docstring.'''
    pass

def undocumented_function():
    pass

class MyClass:
    '''Class docstring.'''
    
    def method_with_doc(self):
        '''Method docstring.'''
        pass
    
    def method_without_doc(self):
        pass
"""
    
    tree = ast.parse(code)
    
    # Analyze functions
    functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    functions_with_docs = [f for f in functions if ast.get_docstring(f)]
    
    total = len(functions)
    documented = len(functions_with_docs)
    
    print(f"\nTotal functions: {total}")
    print(f"Documented: {documented} ({documented/total*100:.1f}%)")
    
    print("\nDocumented:")
    for func in functions_with_docs:
        print(f"  ✓ {func.name}")
    
    print("\nUndocumented:")
    for func in functions:
        if func not in functions_with_docs:
            print(f"  ✗ {func.name}")


def example_6_code_transformation():
    """Example 6: Modify code using AST (simple example)."""
    print("\n" + "="*70)
    print("EXAMPLE 6: Code Transformation")
    print("="*70)
    
    code = """
x = 1 + 2
y = 3 * 4
z = x + y
"""
    
    print("\nOriginal code:")
    print(code)
    
    tree = ast.parse(code)
    
    # Create a visitor that replaces addition with subtraction
    class AddToSubTransformer(ast.NodeTransformer):
        def visit_BinOp(self, node):
            # Replace Add operations with Sub
            if isinstance(node.op, ast.Add):
                node.op = ast.Sub()
            return node
    
    # Transform the tree
    transformer = AddToSubTransformer()
    new_tree = transformer.visit(tree)
    
    # Convert back to code
    new_code = ast.unparse(new_tree)
    
    print("\nTransformed code (+ changed to -):")
    print(new_code)


def main():
    """Run all AST examples."""
    print("\n" + "="*70)
    print("PYTHON AST (ABSTRACT SYNTAX TREE) EXAMPLES")
    print("="*70)
    print("""
AST allows you to:
1. Parse Python code into a tree structure
2. Analyze code without executing it
3. Extract information (functions, imports, docstrings, type hints)
4. Transform and generate code
5. Build tools like linters, formatters, and analyzers

This is safer than exec() or import because code is never executed!
""")
    
    # Run all examples
    example_1_basic_parsing()
    example_2_extract_functions()
    example_3_analyze_imports()
    example_4_type_hints()
    example_5_docstring_coverage()
    example_6_code_transformation()
    
    print("\n" + "="*70)
    print("Real-world AST tools you might know:")
    print("  - pylint: Code quality checker")
    print("  - black: Code formatter")
    print("  - mypy: Type checker")
    print("  - isort: Import sorter")
    print("  - flake8: Style guide enforcer")
    print("  - Our check_docstring_coverage.py: Documentation analyzer")
    print("="*70 + "\n")


if __name__ == '__main__':
    main()
