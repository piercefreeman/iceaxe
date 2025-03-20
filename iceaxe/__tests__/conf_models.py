from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import EmailStr, HttpUrl, PositiveInt
from pyinstrument import Profiler

from iceaxe.base import Field, TableBase, UniqueConstraint


class UserDemo(TableBase):
    id: int = Field(primary_key=True, default=None)
    name: str
    email: str


class ArtifactDemo(TableBase):
    id: int = Field(primary_key=True, default=None)
    title: str
    user_id: int = Field(foreign_key="userdemo.id")


class ComplexDemo(TableBase):
    id: int = Field(primary_key=True, default=None)
    string_list: list[str]
    json_data: dict[str, str] = Field(is_json=True)


class Employee(TableBase):
    id: int = Field(primary_key=True, default=None)
    email: str = Field(unique=True)
    first_name: str
    last_name: str
    department: str
    salary: float


class Department(TableBase):
    id: int = Field(primary_key=True, default=None)
    name: str = Field(unique=True)
    budget: float
    location: str


class ProjectAssignment(TableBase):
    id: int = Field(primary_key=True, default=None)
    employee_id: int = Field(foreign_key="employee.id")
    project_name: str
    role: str
    start_date: str


class EmployeeStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ON_LEAVE = "on_leave"


class EmployeeMetadata(TableBase):
    id: int = Field(primary_key=True, default=None)
    employee_id: int = Field(foreign_key="employee.id")
    status: EmployeeStatus
    tags: list[str] = Field(is_json=True)
    additional_info: dict[str, Any] = Field(is_json=True)


class FunctionDemoModel(TableBase):
    id: int = Field(primary_key=True, default=None)
    balance: float
    created_at: str
    birth_date: str
    start_date: str
    end_date: str
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    years: int
    months: int
    days: int
    weeks: int
    hours: int
    minutes: int
    seconds: int
    name: str
    balance_str: str
    timestamp_str: str


class DemoModelA(TableBase):
    id: int = Field(primary_key=True, default=None)
    name: str
    description: str
    code: str = Field(unique=True)


class DemoModelB(TableBase):
    id: int = Field(primary_key=True, default=None)
    name: str
    category: str
    code: str = Field(unique=True)


class JsonDemo(TableBase):
    """
    Model for testing JSON field updates.
    """

    id: int | None = Field(primary_key=True, default=None)
    settings: dict[Any, Any] = Field(is_json=True)
    metadata: dict[Any, Any] | None = Field(is_json=True)
    unique_val: str

    table_args = [UniqueConstraint(columns=["unique_val"])]


# Model for testing Annotated types
class AnnotatedTypesModel(TableBase):
    """Model for testing various Annotated types."""

    id: int = Field(primary_key=True)

    # Direct pydantic types
    email: EmailStr
    url: HttpUrl | None
    value: PositiveInt  # PositiveInt


@contextmanager
def run_profile(request):
    TESTS_ROOT = Path.cwd()
    PROFILE_ROOT = TESTS_ROOT / ".profiles"

    # Turn profiling on
    profiler = Profiler()
    profiler.start()

    yield  # Run test

    profiler.stop()
    PROFILE_ROOT.mkdir(exist_ok=True)
    results_file = PROFILE_ROOT / f"{request.node.name}.html"
    profiler.write_html(results_file)
