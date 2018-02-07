from rx.core import Scheduler, Observable

from rx.concurrency import immediate_scheduler


def start_with_accepts_empty(self, *args, **kwargs):
    """Prepends a sequence of values to an observable sequence with an
    optional scheduler and an argument list of values to prepend.

    1 - source.start_with(1, 2, 3)
    2 - source.start_with(Scheduler.timeout, 1, 2, 3)

    Returns the source sequence prepended with the specified values.
    """

    if len(args) > 0 and isinstance(args[0], Scheduler):
        scheduler = args[0]
        args = args[1:]
    else:
        scheduler = kwargs.get("scheduler", immediate_scheduler)

    sequence = [Observable.from_(args, scheduler), self]
    return Observable.concat(sequence)


def patch_observable(Obs):
    Obs.start_with = start_with_accepts_empty
