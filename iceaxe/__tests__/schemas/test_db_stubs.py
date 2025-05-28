import pytest

from iceaxe.schemas.db_stubs import ConstraintPointerInfo, DBObjectPointer, DBType


class MockDBObjectPointer(DBObjectPointer):
    """Mock implementation of DBObjectPointer for testing parser methods."""

    representation_str: str

    def representation(self) -> str:
        return self.representation_str


@pytest.mark.parametrize(
    "representation_str,expected_result",
    [
        # Valid constraint pointer formats
        (
            "users.['id'].PRIMARY KEY",
            ConstraintPointerInfo("users", ["id"], "PRIMARY KEY"),
        ),
        (
            "orders.['user_id', 'product_id'].UNIQUE",
            ConstraintPointerInfo("orders", ["user_id", "product_id"], "UNIQUE"),
        ),
        (
            "products.['name'].INDEX",
            ConstraintPointerInfo("products", ["name"], "INDEX"),
        ),
        (
            "table_name.['col1', 'col2', 'col3'].FOREIGN KEY",
            ConstraintPointerInfo(
                "table_name", ["col1", "col2", "col3"], "FOREIGN KEY"
            ),
        ),
        # Single quotes
        ("users.['email'].UNIQUE", ConstraintPointerInfo("users", ["email"], "UNIQUE")),
        # Double quotes
        ('users.["email"].UNIQUE', ConstraintPointerInfo("users", ["email"], "UNIQUE")),
        # Mixed quotes
        (
            "users.[\"col1\", 'col2'].UNIQUE",
            ConstraintPointerInfo("users", ["col1", "col2"], "UNIQUE"),
        ),
        # Extra whitespace
        (
            "users.[ 'col1' , 'col2' ].UNIQUE",
            ConstraintPointerInfo("users", ["col1", "col2"], "UNIQUE"),
        ),
        # Empty column list
        ("users.[].CHECK", ConstraintPointerInfo("users", [], "CHECK")),
        # Schema-qualified table names (dots in table names are valid when representing schema.table)
        (
            "public.users.['column'].PRIMARY KEY",
            ConstraintPointerInfo("public.users", ["column"], "PRIMARY KEY"),
        ),
        # Complex constraint types
        (
            "users.['id'].PRIMARY KEY AUTOINCREMENT",
            ConstraintPointerInfo("users", ["id"], "PRIMARY KEY AUTOINCREMENT"),
        ),
        # Table names with underscores and numbers (valid PostgreSQL identifiers)
        (
            "user_table_2.['id'].PRIMARY KEY",
            ConstraintPointerInfo("user_table_2", ["id"], "PRIMARY KEY"),
        ),
        # Column names with underscores and numbers
        (
            "users.['user_id_2', 'created_at'].UNIQUE",
            ConstraintPointerInfo("users", ["user_id_2", "created_at"], "UNIQUE"),
        ),
        # Invalid formats that should return None
        ("users.column.UNIQUE", None),  # Missing brackets
        ("users.['column']", None),  # Missing constraint type
        ("['column'].UNIQUE", None),  # Missing table name
        ("users", None),  # Just table name
        ("", None),  # Empty string
        ("users.column", None),  # Simple table.column format
        ("invalid_format", None),  # Random string
        # Malformed bracket syntax
        ("users.[column].UNIQUE", None),  # Missing quotes in brackets
        ("users.['column.UNIQUE", None),  # Unclosed bracket
        ("users.column'].UNIQUE", None),  # Missing opening bracket
    ],
)
def test_parse_constraint_pointer(
    representation_str: str, expected_result: ConstraintPointerInfo | None
):
    """Test parsing of constraint pointer representations."""
    pointer = MockDBObjectPointer(representation_str=representation_str)
    result = pointer.parse_constraint_pointer()

    if expected_result is None:
        assert result is None
    else:
        assert result is not None
        assert result.table_name == expected_result.table_name
        assert result.column_names == expected_result.column_names
        assert result.constraint_type == expected_result.constraint_type


@pytest.mark.parametrize(
    "representation_str,expected_table_name",
    [
        # Constraint pointer formats
        ("users.['id'].PRIMARY KEY", "users"),
        ("orders.['user_id', 'product_id'].UNIQUE", "orders"),
        ("public.users.['column'].INDEX", "public.users"),
        # Simple table.column formats
        ("users.email", "users"),
        ("products.name", "products"),
        ("public.users.column", "public.users"),  # Schema.table.column format
        # Edge cases
        ("table_only", "table_only"),
        ("", None),  # Empty string should return None
        ("users.['id'].PRIMARY KEY", "users"),  # Constraint format takes precedence
        # Complex table names with underscores and numbers
        ("user_table_123.column", "user_table_123"),
        ("schema_1.table_2.column", "schema_1.table_2"),
        # Multiple dots in representation (should extract the table part correctly)
        ("very.long.schema.table.['col'].UNIQUE", "very.long.schema.table"),
    ],
)
def test_get_table_name(representation_str: str, expected_table_name: str | None):
    """Test extraction of table names from pointer representations."""
    pointer = MockDBObjectPointer(representation_str=representation_str)
    result = pointer.get_table_name()
    assert result == expected_table_name


@pytest.mark.parametrize(
    "representation_str,expected_column_names",
    [
        # Constraint pointer formats
        ("users.['id'].PRIMARY KEY", ["id"]),
        ("orders.['user_id', 'product_id'].UNIQUE", ["user_id", "product_id"]),
        ("products.['name', 'category', 'price'].INDEX", ["name", "category", "price"]),
        ("users.[].CHECK", []),  # Empty column list
        # Simple table.column formats
        ("users.email", ["email"]),
        ("products.name", ["name"]),
        ("public.users.column", ["column"]),  # Schema.table.column format
        # Edge cases
        ("table_only", []),  # No columns
        ("", []),  # Empty string
        # Whitespace handling
        ("users.[ 'col1' , 'col2' ].UNIQUE", ["col1", "col2"]),
        # Quote handling
        ("users.[\"col1\", 'col2'].UNIQUE", ["col1", "col2"]),
        # Column names with underscores and numbers
        (
            "users.['user_id_2', 'created_at_timestamp'].UNIQUE",
            ["user_id_2", "created_at_timestamp"],
        ),
        # Complex schema.table.column cases
        ("schema.table.column_name", ["column_name"]),
        ("very.long.schema.table.column", ["column"]),
    ],
)
def test_get_column_names(representation_str: str, expected_column_names: list[str]):
    """Test extraction of column names from pointer representations."""
    pointer = MockDBObjectPointer(representation_str=representation_str)
    result = pointer.get_column_names()
    assert result == expected_column_names


def test_merge_type_columns():
    """
    Allow separately yielded type definitions to collect their reference columns. If an
    enum is referenced in one place, this should build up to the full definition.

    """
    type_a = DBType(
        name="type_a",
        values=frozenset({"A"}),
        reference_columns=frozenset({("table_a", "column_a")}),
    )
    type_b = DBType(
        name="type_a",
        values=frozenset({"A"}),
        reference_columns=frozenset({("table_b", "column_b")}),
    )

    merged = type_a.merge(type_b)
    assert merged.name == "type_a"
    assert merged.values == frozenset({"A"})
    assert merged.reference_columns == frozenset(
        {("table_a", "column_a"), ("table_b", "column_b")}
    )
