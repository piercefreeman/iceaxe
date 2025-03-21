import ast
import inspect
import os
from contextlib import contextmanager
from dataclasses import dataclass
from json import JSONDecodeError, dump as json_dump, loads as json_loads
from re import Pattern
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent

from pyright import run


@dataclass
class PyrightDiagnostic:
    file: str
    severity: str
    message: str
    rule: str | None
    line: int
    column: int


class ExpectedPyrightError(Exception):
    """
    Exception raised when Pyright doesn't produce the expected error

    """

    pass


def get_imports_from_module(module_source: str) -> set[str]:
    """
    Extract all import statements from module source

    """
    tree = ast.parse(module_source)
    imports: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                imports.add(f"import {name.name}")
        elif isinstance(node, ast.ImportFrom):
            names = ", ".join(name.name for name in node.names)
            if node.module is None:
                # Handle "from . import x" case
                imports.add(f"from . import {names}")
            else:
                imports.add(f"from {node.module} import {names}")

    return imports


def strip_type_ignore(line: str) -> str:
    """
    Strip type: ignore comments from a line while preserving the line content

    """
    if "#" not in line:
        return line

    # Split only on the first #
    code_part, *comment_parts = line.split("#", 1)
    if not comment_parts:
        return line

    comment = comment_parts[0]
    # If this is a type: ignore comment, return just the code
    if "type:" in comment and "ignore" in comment:
        return code_part.rstrip()

    # Otherwise return the full line
    return line


def extract_current_function_code():
    """
    Extracts the source code of the function calling this utility,
    along with any necessary imports at the module level. This only works for
    functions in a pytest testing context that are prefixed with `test_`.

    """
    # Get the frame of the calling function
    frame = inspect.currentframe()

    try:
        # Go up until we find the test function; workaround to not
        # knowing the entrypoint of our contextmanager at runtime
        while frame is not None:
            func_name = frame.f_code.co_name
            if func_name.startswith("test_"):
                test_frame = frame
                break
            frame = frame.f_back
        else:
            raise RuntimeError("Could not find test function frame")

        # Source code of the function
        func_source = inspect.getsource(test_frame.f_code)

        # Source code of the larger test file, which contains the test function
        # All the imports used by the test function should be within this file
        module = inspect.getmodule(test_frame)
        if not module:
            raise RuntimeError("Could not find module for test function")

        module_source = inspect.getsource(module)

        # Postprocess the source code to build into a valid new module
        imports = get_imports_from_module(module_source)
        filtered_lines = [strip_type_ignore(line) for line in func_source.split("\n")]
        return "\n".join(sorted(imports)) + "\n\n" + dedent("\n".join(filtered_lines))

    finally:
        del frame  # Avoid reference cycles


def create_pyright_config():
    """
    Creates a new pyright configuration that ignores unused imports or other
    issues that are not related to context-manager wrapped type checking.

    """
    return {
        "include": ["."],
        "exclude": [],
        "ignore": [],
        "strict": [],
        "typeCheckingMode": "strict",
        "reportUnusedImport": False,
        "reportUnusedVariable": False,
        # Focus only on type checking
        "reportOptionalMemberAccess": True,
        "reportGeneralTypeIssues": True,
        "reportPropertyTypeMismatch": True,
        "reportFunctionMemberAccess": True,
        "reportTypeCommentUsage": True,
        "reportMissingTypeStubs": False,
        # Only typehint intentional typehints, not inferred values
        "reportUnknownParameterType": False,
        "reportUnknownVariableType": False,
        "reportUnknownMemberType": False,
        "reportUnknownArgumentType": False,
        "reportMissingParameterType": False,
        "extraPaths": [
            os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        ],
        "reportAttributeAccessIssue": True,
        "reportArgumentType": True,
    }


def run_pyright(file_path: str) -> list[PyrightDiagnostic]:
    """
    Run pyright on a file and return the diagnostics

    """
    try:
        with TemporaryDirectory() as temp_dir:
            # Create pyright config
            config_path = os.path.join(temp_dir, "pyrightconfig.json")
            with open(config_path, "w") as f:
                json_dump(create_pyright_config(), f)

            # Copy the file to analyze into the project directory
            test_file = os.path.join(temp_dir, "test.py")
            with open(file_path, "r") as src, open(test_file, "w") as dst:
                dst.write(src.read())

            # Run pyright with the config
            result = run(
                "--project",
                temp_dir,
                "--outputjson",
                test_file,
                capture_output=True,
                text=True,
            )

            try:
                output = json_loads(result.stdout)
            except JSONDecodeError:
                print(f"Failed to parse pyright output: {result.stdout}")  # noqa: T201
                print(f"Stderr: {result.stderr}")  # noqa: T201
                raise

            if "generalDiagnostics" not in output:
                raise RuntimeError(
                    f"Unknown pyright output, missing generalDiagnostics: {output}"
                )

            diagnostics: list[PyrightDiagnostic] = []
            for diag in output["generalDiagnostics"]:
                diagnostics.append(
                    PyrightDiagnostic(
                        file=diag["file"],
                        severity=diag["severity"],
                        message=diag["message"],
                        rule=diag.get("rule"),
                        line=diag["range"]["start"]["line"] + 1,  # Convert to 1-based
                        column=(
                            diag["range"]["start"]["character"]
                            + 1  # Convert to 1-based
                        ),
                    )
                )

            return diagnostics

    except Exception as e:
        raise RuntimeError(f"Failed to run pyright: {str(e)}")


@contextmanager
def pyright_raises(
    expected_rule: str,
    expected_line: int | None = None,
    matches: Pattern | None = None,
):
    """
    Context manager that verifies code produces a specific Pyright error.

    :params expected_rule: The Pyright rule that should be violated
    :params expected_line: Optional line number where the error should occur

    :raises ExpectedPyrightError: If Pyright doesn't produce the expected error

    """
    # Create a temporary file to store the code
    with NamedTemporaryFile(mode="w", suffix=".py") as temp_file:
        temp_path = temp_file.name

        # Extract the source code of the calling function
        source_code = extract_current_function_code()
        print(f"Running Pyright on:\n{source_code}")  # noqa: T201

        # Write the source code to the temporary file
        temp_file.write(source_code)
        temp_file.flush()

        # At runtime, our actual code is probably a no-op but we still let it run
        # inside the scope of the contextmanager
        yield

        # Run Pyright on the temporary file
        diagnostics = run_pyright(temp_path)

        # Check if any of the diagnostics match our expected error
        for diagnostic in diagnostics:
            if diagnostic.rule == expected_rule:
                if expected_line is not None and diagnostic.line != expected_line:
                    continue
                if matches and not matches.search(diagnostic.message):
                    continue
                # Found matching error
                return

        # If we get here, we didn't find the expected error
        actual_errors = [
            f"{d.rule or 'unknown'} on line {d.line}: {d.message}" for d in diagnostics
        ]
        raise ExpectedPyrightError(
            f"Expected Pyright error {expected_rule}"
            f"{f' on line {expected_line}' if expected_line else ''}"
            f" but got: {', '.join(actual_errors) if actual_errors else 'no errors'}"
        )
