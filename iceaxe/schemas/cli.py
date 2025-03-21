from inspect import isclass
from typing import Type

from iceaxe.base import DBModelMetaclass, TableBase
from iceaxe.schemas.actions import DatabaseActions
from iceaxe.schemas.db_memory_serializer import DatabaseMemorySerializer
from iceaxe.session import DBConnection


async def create_all(
    db_connection: DBConnection, models: list[Type[TableBase]] | None = None
):
    # Get all of the instances that have been registered
    # in memory scope by the user.
    if models is None:
        models = [
            cls
            for cls in DBModelMetaclass.get_registry()
            if isclass(cls) and issubclass(cls, TableBase)
        ]

    # Find all polymorphic models and their subclasses
    all_models = list(models)  # Copy to avoid modifying the original list
    for model in models:
        # If this is a polymorphic base class, add all its subclasses
        if (
            hasattr(model, "get_discriminator_field")
            and model.get_discriminator_field() is not None
        ):
            if hasattr(model, "get_all_subclasses"):
                subclasses = model.get_all_subclasses()
                for subclass in subclasses:
                    if subclass not in all_models:
                        all_models.append(subclass)

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate(all_models))
    next_ordering = migrator.order_db_objects(db_objects)

    # Create the tables in the database
    actor = DatabaseActions(dry_run=False, db_connection=db_connection)
    await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )
