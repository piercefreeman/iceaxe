# iceaxe

![Iceaxe Logo](https://raw.githubusercontent.com/piercefreeman/iceaxe/main/media/header.png)

![Python Version](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fpiercefreeman%2Ficeaxe%2Frefs%2Fheads%2Fmain%2Fpyproject.toml) [![Test status](https://github.com/piercefreeman/iceaxe/actions/workflows/test.yml/badge.svg)](https://github.com/piercefreeman/iceaxe/actions)

A modern, fast ORM for Python. We have the following goals:

- 🏎️ **Performance**: We want to exceed or match the fastest ORMs in Python. We want our ORM
to be as close as possible to raw-[asyncpg](https://github.com/MagicStack/asyncpg) speeds. See the "Benchmarks" section for more.
- 📝 **Typehinting**: Everything should be typehinted with expected types. Declare your data as you
expect in Python and it should bidirectionally sync to the database.
- 🐘 **Postgres only**: Leverage native Postgres features and simplify the implementation.
- ⚡ **Common things are easy, rare things are possible**: 99% of the SQL queries we write are
vanilla SELECT/INSERT/UPDATEs. These should be natively supported by your ORM. If you're writing _really_
complex queries, these are better done by hand so you can see exactly what SQL will be run.

Iceaxe is used in production at several companies. It's also an independent project. It's compatible with the [Mountaineer](https://github.com/piercefreeman/mountaineer) ecosystem, but you can use it in whatever
project and web framework you're using.

For comprehensive documentation, visit [https://iceaxe.sh](https://iceaxe.sh).

To auto-optimize your self hosted Postgres install, check out our new [autopg](https://github.com/piercefreeman/autopg) project.

## Installation

If you're using poetry to manage your dependencies:

```bash
uv add iceaxe
```

Otherwise install with pip:

```bash
pip install iceaxe
```

## Usage

Define your models as a `TableBase` subclass:

```python
from iceaxe import TableBase

class Person(TableBase):
    id: int
    name: str
    age: int
```

TableBase is a subclass of Pydantic's `BaseModel`, so you get all of the validation and Field customization
out of the box. We provide our own `Field` constructor that adds database-specific configuration. For instance, to make the
`id` field a primary key / auto-incrementing you can do:

```python
from iceaxe import Field

class Person(TableBase):
    id: int = Field(primary_key=True)
    name: str
    age: int
```

Okay now you have a model. How do you interact with it?

Databases are based on a few core primitives to insert data, update it, and fetch it out again.
To do so you'll need a _database connection_, which is a connection over the network from your code
to your Postgres database. The `DBConnection` is the core class for all ORM actions against the database.

```python
from iceaxe import DBConnection
import asyncpg

conn = DBConnection(
    await asyncpg.connect(
        host="localhost",
        port=5432,
        user="db_user",
        password="yoursecretpassword",
        database="your_db",
    )
)
```

The Person class currently just lives in memory. To back it with a full
database table, we can run raw SQL or run a migration to add it:

```python
await conn.conn.execute(
    """
    CREATE TABLE IF NOT EXISTS person (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        age INT NOT NULL
    )
    """
)
```

### Inserting Data

Instantiate object classes as you normally do:

```python
people = [
    Person(name="Alice", age=30),
    Person(name="Bob", age=40),
    Person(name="Charlie", age=50),
]
await conn.insert(people)

print(people[0].id) # 1
print(people[1].id) # 2
```

Because we're using an auto-incrementing primary key, the `id` field will be populated after the insert.
Iceaxe will automatically update the object in place with the newly assigned value.

### Updating data

Now that we have these lovely people, let's modify them.

```python
person = people[0]
person.name = "Blice"
```

Right now, we have a Python object that's out of state with the database. But that's often okay. We can inspect it
and further write logic - it's fully decoupled from the database.

```python
def ensure_b_letter(person: Person):
    if person.name[0].lower() != "b":
        raise ValueError("Name must start with 'B'")

ensure_b_letter(person)
```

To sync the values back to the database, we can call `update`:

```python
await conn.update([person])
```

If we were to query the database directly, we see that the name has been updated:

```
id | name  | age
----+-------+-----
  1 | Blice |  31
  2 | Bob   |  40
  3 | Charlie | 50
```

But no other fields have been touched. This lets a potentially concurrent process
modify `Alice`'s record - say, updating the age to 31. By the time we update the data, we'll
change the name but nothing else. Under the hood we do this by tracking the fields that
have been modified in-memory and creating a targeted UPDATE to modify only those values.

### Selecting data

To select data, we can use a `QueryBuilder`. For a shortcut to `select` query functions,
you can also just import select directly. This method takes the desired value parameters
and returns a list of the desired objects.

```python
from iceaxe import select

query = select(Person).where(Person.name == "Blice", Person.age > 25)
results = await conn.exec(query)
```

If we inspect the typing of `results`, we see that it's a `list[Person]` objects. This matches
the typehint of the `select` function. You can also target columns directly:

```python
query = select((Person.id, Person.name)).where(Person.age > 25)
results = await conn.exec(query)
```

This will return a list of tuples, where each tuple is the id and name of the person: `list[tuple[int, str]]`.

We support most of the common SQL operations. Just like the results, these are typehinted
to their proper types as well. Static typecheckers and your IDE will throw an error if you try to compare
a string column to an integer, for instance. A more complex example of a query:

```python
query = select((
    Person.id,
    FavoriteColor,
)).join(
    FavoriteColor,
    Person.id == FavoriteColor.person_id,
).where(
    Person.age > 25,
    Person.name == "Blice",
).order_by(
    Person.age.desc(),
).limit(10)
results = await conn.exec(query)
```

As expected this will deliver results - and typehint - as a `list[tuple[int, FavoriteColor]]`

## Production

> [!IMPORTANT]
> Iceaxe is in early alpha. We're using it internally and showly rolling out to our production
applications, but we're not yet ready to recommend it for general use. The API and larger
stability is subject to change.

Note that underlying Postgres connection wrapped by `conn` will be alive for as long as your object is in memory. This uses up one
of the allowable connections to your database. Your overall limit depends on your Postgres configuration
or hosting provider, but most managed solutions top out around 150-300. If you need more concurrent clients
connected (and even if you don't - connection creation at the Postgres level is expensive), you can adopt
a load balancer like `pgbouncer` to better scale to traffic. More deployment notes to come.

It's also worth noting the absence of request pooling in this initialization. This is a feature of many ORMs that lets you limit
the overall connections you make to Postgres, and re-use these over time. We specifically don't offer request
pooling as part of Iceaxe, despite being supported by our underlying engine `asyncpg`. This is a bit more
aligned to how things should be structured in production. Python apps are always bound to one process thanks to
the GIL. So no matter what your connection pool will always be tied to the current Python process / runtime. When you're deploying onto a server with multiple cores, the pool will be duplicated across CPUs and largely defeats the purpose of capping
network connections in the first place.

## Benchmarking

We have basic benchmarking tests in the `__tests__/benchmarks` directory. To run them, you'll need to execute the pytest suite:

```bash
uv run pytest -m integration_tests
```

Current benchmarking as of October 11 2024 is:

|                   | raw asyncpg | iceaxe | external overhead                             |   |
|-------------------|-------------|--------|-----------------------------------------------|---|
| TableBase columns | 0.098s      | 0.093s |                                               |   |
| TableBase full    | 0.164s      | 1.345s | 10%: dict construction | 90%: pydantic overhead |   |

## Development

If you update your Cython implementation during development, you'll need to re-compile the Cython code. This can be done with
a simple uv sync.

```bash
uv sync
```