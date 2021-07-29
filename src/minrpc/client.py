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


# Needed for py2 compatibility, otherwise could just use contextlib.ExitStack:
class NoLock(object):

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


class RemoteProcessClosed(RuntimeError):
    """The MAD-X remote process has already been closed."""
    pass


class RemoteProcessCrashed(RuntimeError):
    """The MAD-X remote process has crashed."""
    pass


class Client(object):

    """
    Base class for a very lightweight synchronous RPC client.

    Uses a connection that shares the interface with socket to
    do synchronous RPC. Synchronous IO means that currently callbacks /
    events are impossible.
    """

    module = 'minrpc.service'

    def __init__(self, sock, lock=None, proc=None):
        """Initialize the client with a socket object."""
        self._conn = sock
        self._good = True
        self._lock = lock or NoLock()
        self._proc = proc

    def __del__(self):
        """Close the client and the associated connection with it."""
        try:
            self.close()
        except (RemoteProcessCrashed, RemoteProcessClosed,
                IOError, EOFError, OSError, ValueError):
            # catch ugly follow-up warnings after a MAD-X process has crashed
            pass

    def __bool__(self):
        return self._good and not self.closed

    __nonzero__ = __bool__
    good = property(__bool__)

    @classmethod
    def spawn_subprocess(cls, lock=None, **Popen_args):
        """
        Create client for a backend service in a subprocess.

        You can use the keyword arguments to pass further arguments to
        Popen, which is useful for example, if you want to redirect STDIO
        streams.
        """
        args = [sys.executable, '-m', cls.module]
        sock, proc = ipc.spawn_subprocess(args, **Popen_args)
        return cls(sock, lock=lock, proc=proc), proc

    @classmethod
    def fork_client(cls, client):
        """
        Forks the given client.

        This is achieved by calling the 'fork' dispatcher on the remote
            service, and replacing the existing socket IPC connection
            for a new one.
        """
        client._request("fork")
        new_socket_local, new_socket_remote = ipc.create_socketpair()
        client._conn.send_fd(new_socket_remote.fileno())
        _, (res,) = new_socket_local.recv()
        if res != "ready!":
            raise RuntimeError
        new_socket_remote.close()
        client._conn.send("done")
        return cls(new_socket_local, client._lock, client._proc)

    def close(self):
        """Close the connection gracefully, stop the remote service."""
        if self.good:
            self._conn.send(('close', ()))
        self._conn.close()
        # if self._proc:
            # self._proc.wait()

    @property
    def closed(self):
        """Check if connection is closed."""
        return self._conn.closed()

    def _request(self, kind, *args):
        """Communicate with the remote service synchronously."""
        with self._lock:
            if self.closed:
                raise RemoteProcessClosed()
            if not self._good:
                raise RemoteProcessCrashed()
            try:
                response = self._communicate((kind, args))
            except (IOError, EOFError, OSError, ValueError):
                self._good = False
                self._conn.close()
                raise RemoteProcessCrashed()
        return self._dispatch(response)

    def _communicate(self, message):
        """Transmit one message and wait for the answer."""
        self._conn.send(message)
        return self._conn.recv()

    def _dispatch(self, response):
        """Dispatch an answer from the remote service."""
        kind, args = response
        handler = getattr(self, '_dispatch_%s' % (kind,))
        return handler(*args)

    def _dispatch_exception(self, exc_type, message):
        """Dispatch an exception."""
        # Raise a wrapper exception type to avoid problems if the constructor
        # expects a different arguments than a message string:
        raise type(exc_type.__name__, (exc_type,), {
            '__str__': lambda *args: message,
            '__init__': lambda *args: None})

    def _dispatch_data(self, data):
        """Dispatch returned data."""
        return data

    def get_module(self, qualname):
        """Get proxy for module in the remote process."""
        return RemoteModule(self, qualname)


class RemoteModule(object):

    """Wrapper for :mod:`cpymad.libmadx` in a remote process."""

    def __init__(self, client, module):
        """Store the client connection."""
        self.__client = client
        self.__module = module

    def __bool__(self):
        return bool(self.__client)

    __nonzero__ = __bool__

    def __getattr__(self, funcname):
        """Resolve all attribute accesses as remote function calls."""
        def DeferredMethod(*args, **kwargs):
            return self.__client._request('function_call', self.__module,
                                          funcname, args, kwargs)
        return DeferredMethod
