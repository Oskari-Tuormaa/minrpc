"""
RPC client utilities.
"""

from __future__ import absolute_import

import sys

from . import ipc


__all__ = [
    'RemoteProcessClosed',
    'RemoteProcessCrashed',
    'Client',
]


class RemoteProcessClosed(RuntimeError):
    """The MAD-X remote process has already been closed."""
    pass


class RemoteProcessCrashed(RuntimeError):
    """The MAD-X remote process has crashed."""
    pass


class Client(object):

    """
    Base class for a very lightweight synchronous RPC client.

    Uses a connection that shares the interface with :class:`Connection` to
    do synchronous RPC. Synchronous IO means that currently callbacks /
    events are impossible.
    """

    module = 'minrpc.service'

    def __init__(self, conn):
        """Initialize the client with a :class:`Connection` like object."""
        self._conn = conn

    def __del__(self):
        """Close the client and the associated connection with it."""
        try:
            self.close()
        except (RemoteProcessCrashed, RemoteProcessClosed):
            # catch ugly follow-up warnings after a MAD-X process has crashed
            pass

    @classmethod
    def spawn_subprocess(cls, **Popen_args):
        """
        Create client for a backend service in a subprocess.

        You can use the keyword arguments to pass further arguments to
        Popen, which is useful for example, if you want to redirect STDIO
        streams.
        """
        args = [sys.executable, '-m', cls.module]
        conn, proc = ipc.spawn_subprocess(args, **Popen_args)
        return cls(conn), proc

    def close(self):
        """Close the connection gracefully, stop the remote service."""
        try:
            self._conn.send(('close', ()))
        except ValueError:      # already closed
            pass
        self._conn.close()

    @property
    def closed(self):
        """Check if connection is closed."""
        return self._conn.closed

    def _request(self, kind, *args):
        """Communicate with the remote service synchronously."""
        try:
            self._conn.send((kind, args))
        except ValueError:
            if self.closed:
                raise RemoteProcessClosed()
            raise
        except IOError:
            raise RemoteProcessCrashed()
        try:
            response = self._conn.recv()
        except EOFError:
            raise RemoteProcessCrashed()
        return self._dispatch(response)

    def _dispatch(self, response):
        """Dispatch an answer from the remote service."""
        kind, args = response
        handler = getattr(self, '_dispatch_%s' % (kind,))
        return handler(*args)

    def _dispatch_exception(self, exc_type, message):
        """Dispatch an exception."""
        raise Exception(exc_type.__name__ + "\n" + message)

    def _dispatch_data(self, data):
        """Dispatch returned data."""
        return data

    @property
    class modules(object):

        """Provides access to all modules in the remote process."""

        def __init__(self, client):
            self.__client = client

        def __getitem__(self, key):
            """Get a RemoteModule object by module name."""
            return RemoteModule(self.__client, key)


class RemoteModule(object):

    """Wrapper for :mod:`cpymad.libmadx` in a remote process."""

    def __init__(self, client, module):
        """Store the client connection."""
        self.__client = client
        self.__module = module

    def __getattr__(self, funcname):
        """Resolve all attribute accesses as remote function calls."""
        def DeferredMethod(*args, **kwargs):
            return self.__client._request('function_call', self.__module,
                                          funcname, args, kwargs)
        return DeferredMethod
