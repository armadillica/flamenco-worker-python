"""Classes for JSON documents used in upstream communication."""

import attr


@attr.s(auto_attribs=True)
class Activity:
    """Activity on a task."""

    activity: str = ''
    current_command_idx: int = 0
    task_progress_percentage: int = 0
    command_progress_percentage: int = 0
    metrics: dict = {}


@attr.s
class MayKeepRunningResponse:
    """Response from the /may-i-run/{task-id} endpoint"""

    may_keep_running = attr.ib(
        validator=attr.validators.instance_of(bool))
    reason = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)))
    status_requested = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)))


@attr.s
class StatusChangeRequest:
    """Response from the /task endpoint when we're requested to change our status"""

    status_requested = attr.ib(validator=attr.validators.instance_of(str))
