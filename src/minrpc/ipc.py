"""
IPC utilities.
"""

from __future__ import absolute_import

import os
import subprocess
import sys

from .connection import Connection

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


def spawn_subprocess(argv, **Popen_args):
    """
    Spawn a subprocess and pass to it two IPC handles.

    You can use the keyword arguments to pass further arguments to
    Popen, which is useful for example, if you want to redirect STDIO
    streams.

    Return (ipc_connection, process).
    """
    conn, remote_recv, remote_send = create_ipc_connection()
    args = argv + [str(int(remote_recv)), str(int(remote_send))]
    with open(os.devnull, 'w+') as devnull:
        for stream in ('stdout', 'stderr', 'stdin'):
            # Check whether it was explicitly disabled (`False`) rather than
            # simply not specified (`None`)?
            if Popen_args.get(stream) is False:
                Popen_args[stream] = devnull
        proc = subprocess.Popen(args, close_fds=False, **Popen_args)
    # wait for subprocess to confirm that all handles are closed:
    if conn.recv() != 'ready':
        raise RuntimeError
    return conn, proc


def prepare_subprocess_ipc(args):
    """
    Prepare this process for IPC with its parent. Close all the open handles
    except for the STDIN/STDOUT/STDERR and the IPC handles. Return a
    :class:`Connection` to the parent process.
    """
    handles = [Handle(int(arg)) for arg in args]
    recv_fd = handles[0].detach_fd()
    send_fd = handles[1].detach_fd()
    conn = Connection.from_fd(recv_fd, send_fd)
    close_all_but([sys.stdin.fileno(),
                   sys.stdout.fileno(),
                   sys.stderr.fileno(),
                   recv_fd, send_fd])
    conn.send('ready')
    return conn
