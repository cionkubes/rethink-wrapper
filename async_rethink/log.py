from logging import Handler, Formatter

import rethinkdb as r
import traceback


class RethinkEventsLogger(Handler):
    def __init__(self, insert_fn):
        super().__init__()

        self.insert = insert_fn

    def emit(self, record):
        self.insert({
            **self.format(record),
            'event': 'log',
            'status': 'log',
            'time': r.now().to_epoch_time()
        })


class EventsFormatter(Formatter):
    def __init__(self, emitter):
        super().__init__()

        self.emitter = emitter

    def format(self, record):
        log = {
            "level": record.levelno,
            "message": record.getMessage(),
            "emitter": f"{self.emitter}#{record.pathname}:{record.lineno}"
        }

        if record.levelname:
            log['level-name'] = record.levelname

        if record.exc_info:
            log['exception'] = "".join(traceback.format_exception(*record.exc_info))

        return log
