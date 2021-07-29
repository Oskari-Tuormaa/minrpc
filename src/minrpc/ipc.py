"""
IPC utilities.
"""

from __future__ import absolute_import

import array
import os
import socket
import subprocess
import sys

from .connection import Connection, SerializedSocket

py2 = sys.version_info[0] == 2
win = sys.platform == 'win32'

if win:
    from .windows import Handle
else:
    from .posix import Handle


__all__ = [
    'close_all_but',
    'create_ipc_connection',
    'spawn_subprocess',
    'prepare_subprocess_ipc',
]


# On python2/windows, open() creates a non-inheritable file descriptor with an
# underlying inheritable file HANDLE. Therefore, we need to keep track of all
# open files to close their handles in the remote process:
if win and py2:
    from . import file_monitor

    file_monitor.monkey_patch()

    def _get_open_file_handles():
        """Return open file handles as list of ints."""
        return [int(Handle.from_fd(f.fileno(), own=False))
                for f in file_monitor.File._instances if not f.closed]

else:
    def _get_open_file_handles():
        return []


def get_max_fd():
    """Return the maximum possible file descriptor or a wild guess."""
    if not win:
        import resource
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard != resource.RLIM_INFINITY:
            return hard
    try:
        return subprocess.MAXFD
    except AttributeError:          # on py3.5
        return 4096


def close_all_but(keep):
    """Close all but the given file descriptors."""
    # first, let the garbage collector run, it may find some unreachable
    # file objects (on posix forked processes) and close them:
    import gc
    gc.collect()
    # close all ranges in between the file descriptors to be kept:
    keep = sorted(set([-1] + keep + [get_max_fd()]))
    for s, e in zip(keep[:-1], keep[1:]):
        if s+1 < e:
            os.closerange(s+1, e)


def create_ipc_connection():
    """
    Create a connection that can be used for IPC with a subprocess.

    Return (local_connection, remote_recv_handle, remote_send_handle).
    """
    local_recv, _remote_send = Handle.pipe()
    _remote_recv, local_send = Handle.pipe()
    remote_recv = _remote_recv.dup_inheritable()
    remote_send = _remote_send.dup_inheritable()
    conn = Connection.from_fd(local_recv.detach_fd(),
                              local_send.detach_fd())
    return conn, remote_recv, remote_send


def create_socketpair():
    """
    Creates a local/remote socketpair that can be used for
        IPC with a subprocess.

    Return (socket_local, socket_remote)
    """
    s_local, s_remote = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    os.set_inheritable(s_remote.fileno(), True)
    return SerializedSocket(s_local), s_remote


def send_pipe_fds(sock, p_recv, p_send):
    """ Sends pipe fds over socket. """
    socket.send_fds(sock, [" ".encode()], [p_recv, p_send])


def send_socket_fd(sock, sock_fd):
    """ Sends socket fd over socket. """
    sock.sendmsg([" ".encode()], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [sock_fd]))])   


def receive_pipe_fds(sock):
    """
    Receives pipe fds over socket using socket.recv_fds, and returns a new
        connection made from the received fds.

    Return (conn)
    """
    _, (recv_fd, send_fd), _, _ = socket.recv_fds(sock, 1024, 2)
    return Connection.from_fd(recv_fd, send_fd)


def receive_socket_fd(sock):
    """ Receives socket fd over socket. """
    fds = array.array("i")   # Array of ints
    _, ancdata, _, _ = sock.recvmsg(1024, socket.CMSG_LEN(fds.itemsize))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            # Append data, ignoring any truncated integers at the end.
            fds.frombytes(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
    return list(fds)


def spawn_subprocess(argv, **Popen_args):
    """
    Spawn a subprocess and pass to it a socket IPC handle.

    You can use the keyword arguments to pass further arguments to
    Popen, which is useful for example, if you want to redirect STDIO
    streams.

    Return (ipc_serialized_socket, process).
    """
    local_socket, remote_socket = create_socketpair()
    args = argv + [str(int(remote_socket.fileno()))]
    with open(os.devnull, 'w+') as devnull:
        for stream in ('stdout', 'stderr', 'stdin'):
            # Check whether it was explicitly disabled (`False`) rather than
            # simply not specified (`None`)?
            if Popen_args.get(stream) is False:
                Popen_args[stream] = devnull
        proc = subprocess.Popen(args, close_fds=False, **Popen_args)
    local_socket.send(_get_open_file_handles())
    # wait for subprocess to confirm that all handles are closed:
    if local_socket.recv() != 'ready':
        raise RuntimeError
    remote_socket.close()
    return local_socket, proc


def prepare_subprocess_ipc(sock_fd):
    """
    Prepare this process for IPC with its parent. Close all the open handles
    except for the STDIN/STDOUT/STDERR and the IPC handles. Return a
    socket to the parent process.
    """
    sock = SerializedSocket.from_fd(sock_fd)
    close_all_but([sys.stdin.fileno(),
                   sys.stdout.fileno(),
                   sys.stderr.fileno(),
                   sock_fd])
    # On python2/windows open() creates a non-inheritable file descriptor with
    # an underlying inheritable file HANDLE. Since HANDLEs can't be closed
    # with os.closerange, the following snippet is needed to prevent them from
    # staying open in the remote process:
    for handle in sock.recv():
        Handle(handle).close()
    sock.send('ready')
    return sock
