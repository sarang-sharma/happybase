"""
HappyBase Batch module.
"""
import datetime
from collections import defaultdict
import logging
from numbers import Integral
import signal
import time as _time

import six

from Hbase_thrift import BatchMutation, Mutation

from happybase.util import RepeatedTimer

logger = logging.getLogger(__name__)


class Batch(object):
    """Batch mutation class.

    This class cannot be instantiated directly; use :py:meth:`Table.batch`
    instead.
    """

    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False, wal=True, flush_time_interval=None):
        """Initialise a new Batch instance."""
        if not (timestamp is None or isinstance(timestamp, Integral)):
            raise TypeError("'timestamp' must be an integer or None")

        if batch_size is not None:
            if transaction:
                raise TypeError("'transaction' cannot be used when "
                                "'batch_size' is specified")
            if not batch_size > 0:
                raise ValueError("'batch_size' must be > 0")

        if flush_time_interval is not None:
            if transaction:
                raise TypeError("'transaction' cannot be used when "
                                "'flush_time_interval' is specified")
            if not flush_time_interval > 0:
                raise ValueError("'flush_time_interval' must be > 0")
            self._flush_time_interval = flush_time_interval
            self._flush_timer = RepeatedTimer(self._flush_time_interval, self._send_by_timer)

        self._table = table
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._transaction = transaction
        self._wal = wal
        self._families = None
        self._reset_mutations()
        signal.signal(signal.SIGINT, self.handler)

    def _reset_mutations(self):
        """Reset the internal mutation buffer."""
        self._mutations = defaultdict(list)
        self._mutation_count = 0
        if self._flush_timer.is_running:
            self._flush_timer.stop()
        self._last_send = _time.time()

    def _send_by_timer(self):
        """Check if last send time is more than time interval, then send mutations to server."""
        now = _time.time()
        if self._mutation_count > 0 and (_time.time() - self._last_send) * 1000 >= self._flush_time_interval:
            logger.debug("Sending by timer for '%s' (%d mutations)",
                         self._table.name, self._mutation_count)
            self.send()

    def handler(self, signum, frame):
        """Signal handler to send mutations to server."""
        logger.info("Sending by signal '%s' for '%s' (%d mutations)",
                    signum, self._table.name, self._mutation_count)
        self.send()

    def send(self):
        """Send the batch to the server."""
        bms = [
            BatchMutation(row, m)
            for row, m in six.iteritems(self._mutations)
        ]
        if not bms:
            return

        logger.debug("Sending batch for '%s' (%d mutations on %d rows)",
                     self._table.name, self._mutation_count, len(bms))
        if self._timestamp is None:
            self._table.connection.client.mutateRows(self._table.name, bms, {})
        else:
            self._table.connection.client.mutateRowsTs(
                self._table.name, bms, self._timestamp, {})
        self._reset_mutations()

    #
    # Mutation methods
    #

    def put(self, row, data, wal=None):
        """Store data in the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        if wal is None:
            wal = self._wal

        if not self._mutation_count or self._mutation_count == 0:
            self._flush_timer.start()

        self._mutations[row].extend(
            Mutation(
                isDelete=False,
                column=column,
                value=value,
                writeToWAL=wal)
            for column, value in six.iteritems(data))

        self._mutation_count += len(data)
        if self._batch_size and self._mutation_count >= self._batch_size:
            self.send()

    def delete(self, row, columns=None, wal=None):
        """Delete data from the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        # Work-around Thrift API limitation: the mutation API can only
        # delete specified columns, not complete rows, so just list the
        # column families once and cache them for later use by the same
        # batch instance.
        if columns is None:
            if self._families is None:
                self._families = self._table._column_family_names()
            columns = self._families

        if wal is None:
            wal = self._wal

        self._mutations[row].extend(
            Mutation(isDelete=True, column=column, writeToWAL=wal)
            for column in columns)

        self._mutation_count += len(columns)
        if self._batch_size and self._mutation_count >= self._batch_size:
            self.send()

    #
    # Context manager methods
    #

    def __enter__(self):
        """Called upon entering a ``with`` block"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called upon exiting a ``with`` block"""
        # If the 'with' block raises an exception, the batch will not be
        # sent to the server.
        if self._transaction and exc_type is not None:
            return

        self.send()
