import logging
import traceback
from dataclasses import dataclass
from typing import Literal, Sequence, TypeVar

from iceaxe.base import TableBase
from iceaxe.logging import LOGGER

MODIFICATION_TRACKER_VERBOSITY = Literal["ERROR", "WARNING", "INFO"] | None
T = TypeVar("T", bound=TableBase)


@dataclass
class Modification:
    """
    Tracks a single modification to a database model instance, including stack trace information.

    This class stores both the full stack trace and a simplified user-specific stack trace
    that excludes library code. This helps with debugging by showing where in the user's
    code a modification was made.

    :param instance: The model instance that was modified
    :param stack_trace: The complete stack trace at the time of modification
    :param user_stack_trace: The most relevant user code stack trace, excluding library code
    """

    instance: TableBase

    # The full stack trace of the modification.
    stack_trace: str

    # Most specific line of the stack trace that is part of the user's code.
    user_stack_trace: str

    @staticmethod
    def get_current_stack_trace(
        package_allow_list: list[str] | None = None,
        package_deny_list: list[str] | None = None,
    ) -> tuple[str, str]:
        """
        Get both the full stack trace and the most specific user code stack trace.

        The user stack trace filters out library code and frozen code to focus on
        the most relevant user code location where a modification occurred.

        :return: A tuple containing (full_stack_trace, user_stack_trace)
        :rtype: tuple[str, str]
        """
        stack = traceback.extract_stack()[:-1]  # Remove the current frame
        full_trace = "".join(traceback.format_list(stack))

        # Find the most specific user code stack trace by filtering out library code
        user_traces = [
            frame
            for frame in stack
            if (
                package_allow_list is None
                or any(pkg in frame.filename for pkg in package_allow_list)
            )
            and (
                package_deny_list is None
                or not any(pkg in frame.filename for pkg in package_deny_list)
            )
        ]

        user_trace = ""
        if user_traces:
            user_trace = "".join(traceback.format_list([user_traces[-1]]))

        return full_trace, user_trace


class ModificationTracker:
    """
    Tracks modifications to database model instances and manages their lifecycle.

    This class maintains a record of all modified model instances that haven't been
    committed yet. It provides functionality to track new modifications, handle commits,
    and log any remaining uncommitted modifications.

    The tracker organizes modifications by model class and prevents duplicate tracking
    of the same instance. It also captures stack traces at the point of modification
    to help with debugging.
    """

    modified_models: dict[int, Modification]
    """
    Dictionary mapping model classes to lists of their modifications
    """

    verbosity: MODIFICATION_TRACKER_VERBOSITY | None
    """
    The logging level to use when reporting uncommitted modifications
    """

    def __init__(
        self,
        verbosity: MODIFICATION_TRACKER_VERBOSITY | None = None,
        known_first_party: list[str] | None = None,
    ):
        """
        Initialize a new ModificationTracker.

        Creates an empty modification tracking dictionary and sets the initial
        verbosity level to None.
        """
        self.modified_models = {}
        self.verbosity = verbosity
        self.known_first_party = known_first_party

    def track_modification(self, instance: TableBase) -> None:
        """
        Track a modification to a model instance along with its stack trace.

        This method records a modification to a model instance if it hasn't already
        been tracked. It captures both the full stack trace and a user-specific
        stack trace at the point of modification.

        :param instance: The model instance that was modified
        :type instance: TableBase
        """
        # Get stack traces. By default we filter out all iceaxe code, but allow users to override this behavior
        # if we want to still test this logic under test.
        full_trace, user_trace = Modification.get_current_stack_trace(
            package_allow_list=self.known_first_party,
            package_deny_list=["iceaxe"] if not self.known_first_party else None,
        )

        # Only track if we haven't already tracked this instance
        instance_id = id(instance)
        if instance_id not in self.modified_models:
            modification = Modification(
                instance=instance, stack_trace=full_trace, user_stack_trace=user_trace
            )
            self.modified_models[instance_id] = modification

    def clear_status(self, models: Sequence[TableBase]) -> None:
        """
        Remove models that are about to be committed from tracking.

        This method should be called before committing changes to the database.
        It removes the specified models from tracking since they will no longer
        be in an uncommitted state.

        :param models: List of model instances that will be committed
        :type models: list[TableBase]
        """
        for instance in models:
            instance_id = id(instance)
            if instance_id in self.modified_models:
                del self.modified_models[instance_id]

    def log(self) -> None:
        """
        Log all uncommitted modifications with their stack traces.

        This method logs information about all tracked modifications that haven't
        been committed yet. The logging level is determined by the tracker's
        verbosity setting. At the INFO level, it includes the full stack trace
        in addition to the user stack trace.

        If verbosity is not set (None), this method does nothing.
        """
        if not self.verbosity:
            return

        log_level = getattr(logging, self.verbosity)
        LOGGER.setLevel(log_level)  # Ensure logger will capture messages at this level

        for mod in self.modified_models.values():
            LOGGER.log(
                log_level, f"Object modified locally but not committed: {mod.instance}"
            )
            LOGGER.log(log_level, f"Modified at:\n{mod.user_stack_trace}")
            if self.verbosity == "INFO":
                LOGGER.log(log_level, f"Full stack trace:\n{mod.stack_trace}")
