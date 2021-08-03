"""
IPC connection.
"""

from __future__ import absolute_import

import array
import os
import socket
import sys
from struct import Struct

try:
    # python2's cPickle is an accelerated (C extension) version of pickle:
    import cPickle as pickle
except ImportError:
    # python3's pickle automatically uses the accelerated version and falls
    # back to the python version, see:
    # http://docs.python.org/3.3/whatsnew/3.0.html?highlight=cpickle
    import pickle


__all__ = [
    'SerializedSocket',
]

HEADER = Struct("!L")


class SerializedSocket(object):

    """Serialized IPC connection using UNIX socket streams."""

    def __init__(self, socket):
        self._sock = socket

    def recv(self):
        """Receive a pickled message from the remote end."""
        header = recv(self._sock, HEADER.size)
        payload = recv(self._sock, *HEADER.unpack(header))
        return pickle.loads(payload)

    def send(self, data):
        """Send a pickled message to the remote end."""
        # '-1' instructs pickle to use the latest protocol version. This
        # improves performance by a factor ~50-100 in my tests:
        payload = pickle.dumps(data, -1)
        head = HEADER.pack(len(payload))
        self._sock.send(head+payload)

    def send_fd(self, fd):
        """Send a file descriptor to the remote end."""
        self._sock.sendmsg([HEADER.pack(0)], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])

    def recv_fd(self):
        """Receive a file descriptor from the remote end."""
        fds = array.array("i")
        _, ancdata, _, _ = self._sock.recvmsg(HEADER.size, socket.CMSG_LEN(1*fds.itemsize))
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                # Append data, ignoring any truncated integers at the end.
                fds.frombytes(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
        return fds[0]

    def close(self):
        self._sock.close()

    def closed(self):
        """Check if the connection is fully closed."""
        return self._sock.fileno() == -1

    @property
    def socket(self):
        return self._sock

    @classmethod
    def from_fd(cls, sock_fd):
        """Create a connection from a file descriptor."""
        return cls(socket.socket(fileno=sock_fd, family=socket.AF_UNIX, type=socket.SOCK_STREAM))


def recv(file, size):
    """Read a fixed size buffer from """
    parts = []
    while size > 0:
        part = file.recv(size)
        if not part:
            raise EOFError
        parts.append(part)
        size -= len(part)
    return b''.join(parts)
