"""
RPC service utilities.
"""

from __future__ import absolute_import

import logging
import os
import traceback
import sys

from . import ipc
from .connection import SerializedSocket


__all__ = [
    'Service',
]


class Service(object):

    """
    Base class for a very lightweight synchronous RPC service.

    Counterpart to :class:`Client`.
    """

    def __init__(self, sock):
        """Initialize the service with a socket."""
        self._conn = sock

    @classmethod
    def stdio_main(cls, socket_fd):
        """Do the full job of preparing and running an RPC service."""
        sock = ipc.prepare_subprocess_ipc(socket_fd)
        try:
            svc = cls(sock)
            svc.configure_logging()
            svc.run()
        finally:
            sock.close()

    def configure_logging(self):
        """Configure logging module."""
        logging.basicConfig(level=logging.INFO)

    def run(self):
        """
        Run the service until terminated by either the client or user.

        The service is terminated on user interrupts (Ctrl-C), which might
        or might not be desired.
        """
        while self._communicate():
            pass

    def _communicate(self):
        """
        Receive and serve one RPC request.

        :returns: ``True`` if the service should continue running.
        """
        try:
            request = self._conn.recv()
        except (IOError, EOFError, OSError, ValueError):
            return False
        except KeyboardInterrupt:
            # Prevent the child process from exiting prematurely if a
            # KeyboardInterrupt (Ctrl-C) is propagated from the parent
            # process. This is important since the parent process might
            # - not exit at all (e.g. interactive python interpretor!) OR
            # - need to perform clean-up that depends on the child process
            return True
        else:
            return self._dispatch(request)

    def _dispatch(self, request):
        """
        Dispatch one RPC request.

        :returns: ``True`` if the service should continue running.
        """
        kind, args = request
        handler = getattr(self, '_dispatch_%s' % (kind,))
        try:
            response = handler(*args)
        except Exception:
            self._reply_exception(sys.exc_info())
        else:
            try:
                self._reply_data(response)
            except (ValueError, OSError):
                if self._conn.closed():
                    return False
                raise
        return True

    def _dispatch_function_call(self, modname, funcname, args, kwargs):
        """Execute any static function call in the remote process."""
        # As soon as we drop support for python2.6, we should replace this
        # with importlib.import_module:
        module = __import__(modname, None, None, '*')
        function = getattr(module, funcname)
        return function(*args, **kwargs)

    def _dispatch_close(self):
        """Close the connection gracefully as initiated by the client."""
        self._conn.close()

    def _dispatch_fork(self):
        """Fork the service and recieve a new socket for IPC."""
        self._conn.send(('data', (None,)))
        new_conn = SerializedSocket.from_fd(self._conn.recv_fd())
        if os.fork() == 0:
            self._conn = new_conn
            return str(os.P_PID)
        return "ready"

    def _reply_data(self, data):
        """Return data to the client."""
        self._conn.send(('data', (data,)))

    def _reply_exception(self, exc_info):
        """Return an exception state to the client."""
        message = "".join(traceback.format_exception(*exc_info))
        self._conn.send(('exception', (exc_info[0], message)))


if __name__ == '__main__':
    Service.stdio_main(int(sys.argv[1]))
