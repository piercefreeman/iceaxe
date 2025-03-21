from enum import StrEnum

import pytest

from iceaxe import Field, PolymorphicBase
from iceaxe.queries import select
from iceaxe.schemas.cli import create_all
from iceaxe.session import DBConnection


class AnimalType(StrEnum):
    DOG = "dog"
    SMALL_DOG = "small_dog"
    LARGE_DOG = "large_dog"
    CAT = "cat"
    BIRD = "bird"
    MAMMAL = "mammal"


class Animal(PolymorphicBase):
    id: int | None = Field(primary_key=True, default=None)
    type: AnimalType = Field(discriminator_type=True)
    name: str
    age: int


class Mammal(Animal):
    """Base class for mammals with common mammal-specific fields"""

    type: AnimalType = AnimalType.MAMMAL
    fur: bool | None = True  # Made nullable


class Dog(Animal):
    type: AnimalType = AnimalType.DOG
    breed: str | None = None
    bark_volume: int | None = None
    fur: bool | None = True  # Made nullable


class Cat(Animal):
    type: AnimalType = AnimalType.CAT
    fur_color: str | None = None
    lives_left: int | None = None
    fur: bool | None = True  # Made nullable


class Bird(Animal):
    type: AnimalType = AnimalType.BIRD
    wingspan: float | None = None
    can_talk: bool | None = None
    fur: bool | None = None  # Added fur field with null default


class SmallDog(Dog):
    """A more specific type of dog"""

    type: AnimalType = AnimalType.SMALL_DOG  # Explicit discriminator value
    size_category: str | None = "small"
    weight_kg: float | None = None


class LargeDog(Dog):
    """Another specific type of dog"""

    type: AnimalType = AnimalType.LARGE_DOG  # Explicit discriminator value
    size_category: str | None = "large"
    guard_trained: bool | None = False


async def setup_animal_table(db_connection: DBConnection, classes=None):
    """Setup function to prepare the animal table for testing

    Args:
        db_connection: The database connection to use
        classes: Optional list of classes to include in the schema. If None, defaults to [Animal, Dog, Cat, Bird]
    """
    if classes is None:
        classes = [Animal, Dog, Cat, Bird]

    # Drop existing tables and types to ensure clean state
    await db_connection.conn.execute("DROP TABLE IF EXISTS animal")

    # Try to drop the enum type if it exists
    await db_connection.conn.execute('DROP TYPE IF EXISTS "animaltype"')

    # Create table for all animal types
    await create_all(db_connection, classes)

    return db_connection


@pytest.mark.asyncio
async def test_polymorphic_creation_and_loading(db_connection: DBConnection):
    """
    Test that polymorphic models can be created and loaded properly.
    """
    # Setup test environment with only base classes
    await setup_animal_table(db_connection, [Animal, Dog, Cat, Bird])

    # Insert different animal types
    dog = Dog(
        name="Buddy",
        age=5,
        breed="Golden Retriever",
        bark_volume=8,
    )
    cat = Cat(name="Whiskers", age=3, fur_color="Tabby", lives_left=9)
    bird = Bird(name="Polly", age=2, wingspan=0.3, can_talk=True)

    await db_connection.insert([dog, cat, bird])

    # Verify data in database
    animals = await db_connection.exec(select(Animal))

    assert len(animals) == 3

    # Verify each animal is of the correct type
    animal_map = {animal.name: animal for animal in animals}

    assert isinstance(animal_map["Buddy"], Dog)
    assert animal_map["Buddy"].breed == "Golden Retriever"
    assert animal_map["Buddy"].bark_volume == 8

    assert isinstance(animal_map["Whiskers"], Cat)
    assert animal_map["Whiskers"].fur_color == "Tabby"
    assert animal_map["Whiskers"].lives_left == 9

    assert isinstance(animal_map["Polly"], Bird)
    assert animal_map["Polly"].wingspan == 0.3
    assert animal_map["Polly"].can_talk is True


@pytest.mark.asyncio
async def test_polymorphic_query_with_conditions(db_connection: DBConnection):
    """
    Test that polymorphic models can be queried with conditions.
    """
    # Ensure we have data to query
    await setup_animal_table(db_connection, [Animal, Dog, Cat, Bird])

    # Insert test data if needed
    dog = Dog(name="Buddy", age=5, breed="Golden Retriever", bark_volume=8)
    await db_connection.insert([dog])

    # Query only dogs
    dogs = await db_connection.exec(select(Animal).where(Animal.type == AnimalType.DOG))

    assert len(dogs) == 1
    assert isinstance(dogs[0], Dog)
    assert dogs[0].name == "Buddy"

    # Query animals older than 3
    older_animals = await db_connection.exec(select(Animal).where(Animal.age > 3))

    assert len(older_animals) == 1
    assert isinstance(older_animals[0], Dog)


@pytest.mark.asyncio
async def test_polymorphic_update(db_connection: DBConnection):
    """
    Test that polymorphic models can be updated.
    """
    # Ensure we have data to update
    await setup_animal_table(db_connection, [Animal, Dog, Cat, Bird])

    # Insert test data
    dog = Dog(name="Buddy", age=5, breed="Golden Retriever", bark_volume=8)
    await db_connection.insert([dog])

    # Fetch a dog
    dogs = await db_connection.exec(select(Animal).where(Animal.type == AnimalType.DOG))
    dog = dogs[0]

    # Modify the dog
    dog.name = "Max"
    dog.bark_volume = 10

    # Update in the database
    await db_connection.update([dog])

    # Fetch again to verify changes
    updated_dogs = await db_connection.exec(
        select(Animal).where(Animal.type == AnimalType.DOG)
    )
    updated_dog = updated_dogs[0]

    assert isinstance(updated_dog, Dog)
    assert updated_dog.name == "Max"
    assert updated_dog.bark_volume == 10


@pytest.mark.asyncio
async def test_deep_inheritance_hierarchy(db_connection: DBConnection):
    """
    Test polymorphic models with a deeper inheritance hierarchy (Animal -> Dog -> SmallDog/LargeDog).
    Verifies that multi-level polymorphic inheritance works correctly.
    """
    # Register direct discriminator values for subclasses
    Animal._PolymorphicBase__poly_registry["small_dog"] = SmallDog
    Animal._PolymorphicBase__poly_registry["large_dog"] = LargeDog

    # Clear previous data
    await setup_animal_table(
        db_connection, [Animal, Dog, SmallDog, LargeDog, Cat, Bird]
    )

    # Insert different animals including deeper hierarchy
    small_dog = SmallDog(
        name="Fido", age=3, breed="Chihuahua", bark_volume=9, weight_kg=2.5
    )

    large_dog = LargeDog(
        name="Rex", age=5, breed="German Shepherd", bark_volume=10, guard_trained=True
    )

    # Insert models into database
    await db_connection.insert([small_dog, large_dog])

    # Fetch all animals and verify they are loaded with correct types
    animals = await db_connection.exec(select(Animal))

    # Verify we have the right number of animals (the 2 we just inserted)
    assert len(animals) == 2

    # Find our animals by name
    animal_map = {animal.name: animal for animal in animals}

    # Verify the small dog was loaded correctly with all hierarchy fields
    assert isinstance(animal_map["Fido"], SmallDog)
    assert animal_map["Fido"].breed == "Chihuahua"
    assert animal_map["Fido"].bark_volume == 9
    assert animal_map["Fido"].weight_kg == 2.5
    assert animal_map["Fido"].size_category == "small"

    # Verify the large dog was loaded correctly with all hierarchy fields
    assert isinstance(animal_map["Rex"], LargeDog)
    assert animal_map["Rex"].breed == "German Shepherd"
    assert animal_map["Rex"].guard_trained is True
    assert animal_map["Rex"].size_category == "large"


@pytest.mark.asyncio
async def test_polymorphic_batch_operations(db_connection: DBConnection):
    """
    Test batch operations (insert, update, delete) with mixed polymorphic types.
    """
    # Clear previous data
    await setup_animal_table(db_connection, [Animal, Dog, Cat, Bird])

    # Create a batch of different animal types
    animals = (
        [
            Dog(name=f"Dog_{i}", age=i + 1, breed=f"Breed_{i}", bark_volume=i)
            for i in range(5)
        ]
        + [
            Cat(name=f"Cat_{i}", age=i + 1, fur_color=f"Color_{i}", lives_left=9 - i)
            for i in range(5)
        ]
        + [
            Bird(name=f"Bird_{i}", age=i + 1, wingspan=0.1 * i, can_talk=i % 2 == 0)
            for i in range(5)
        ]
    )

    # Batch insert
    await db_connection.insert(animals)

    # Verify all were inserted
    loaded_animals = await db_connection.exec(select(Animal))
    assert len(loaded_animals) == 15

    # Count by type
    dogs = [a for a in loaded_animals if isinstance(a, Dog)]
    cats = [a for a in loaded_animals if isinstance(a, Cat)]
    birds = [a for a in loaded_animals if isinstance(a, Bird)]

    assert len(dogs) == 5
    assert len(cats) == 5
    assert len(birds) == 5

    # Batch update - modify every other animal
    for i, animal in enumerate(loaded_animals):
        if i % 2 == 0:
            animal.name = f"Updated_{animal.name}"

            # Update specific fields based on type
            if isinstance(animal, Dog):
                animal.bark_volume = 100
            elif isinstance(animal, Cat):
                animal.lives_left = 1
            elif isinstance(animal, Bird):
                animal.can_talk = not animal.can_talk

    # Get only the animals we modified
    animals_to_update = [a for i, a in enumerate(loaded_animals) if i % 2 == 0]
    await db_connection.update(animals_to_update)

    # Reload and verify updates
    reloaded_animals = await db_connection.exec(select(Animal))

    for animal in reloaded_animals:
        if animal.name.startswith("Updated_"):
            if isinstance(animal, Dog):
                assert animal.bark_volume == 100
            elif isinstance(animal, Cat):
                assert animal.lives_left == 1
            elif isinstance(animal, Bird):
                # For birds, we just check that can_talk has a value
                assert animal.can_talk is not None

    # Batch delete
    animals_to_delete = [a for a in reloaded_animals if isinstance(a, Cat)]
    await db_connection.delete(animals_to_delete)

    # Verify cats were deleted
    final_animals = await db_connection.exec(select(Animal))
    assert len(final_animals) == 10
    assert not any(isinstance(a, Cat) for a in final_animals)


@pytest.mark.asyncio
async def test_polymorphic_refresh(db_connection: DBConnection):
    """
    Test the refresh behavior for polymorphic models.
    Verifies that refreshing a polymorphic model preserves its correct type.
    """
    # Clear previous data
    await setup_animal_table(db_connection, [Animal, Dog, Cat, Bird])

    # Create and insert a dog
    dog = Dog(name="Rex", age=5, breed="German Shepherd", bark_volume=8)
    await db_connection.insert([dog])

    # In a separate "session", update the dog directly in the database
    await db_connection.conn.execute(
        """
        UPDATE animal
        SET name = 'Updated Rex', age = 6, breed = 'Updated Shepherd', bark_volume = 9
        WHERE id = $1
        """,
        dog.id,
    )

    # Now refresh the original dog object
    await db_connection.refresh([dog])

    # Verify that the dog was refreshed correctly and maintained its type
    assert isinstance(dog, Dog)
    assert dog.name == "Updated Rex"
    assert dog.age == 6
    assert dog.breed == "Updated Shepherd"
    assert dog.bark_volume == 9
