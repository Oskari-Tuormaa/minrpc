from __future__ import absolute_import

# standard library
import os
import tempfile
import unittest

# tested modules
from minrpc.client import Client


class TestRPC(unittest.TestCase):

    # This test is currently only useful for windows. On linux open file
    # descriptors don't prevent the file from being deleted.
    # TODO: change this test such that it checks "directly" whether the file
    # handle is still open.
    def test_no_leaking_file_handles(self):
        fd, filename = tempfile.mkstemp()
        svc, proc = Client.spawn_subprocess()
        os.close(fd)
        try:
            os.remove(filename)
            file_can_be_deleted = True
        except OSError:
            file_can_be_deleted = False
        svc.close()
        proc.wait()
        if not file_can_be_deleted:
            os.remove(filename)
        self.assertTrue(file_can_be_deleted)

    # TODO: add tests to check that other resources get closed correctly.

    def test_fork(self):
        svc1, proc = Client.spawn_subprocess()

        svc2 = Client.fork_client(svc1)

        pid1 = svc1._request("function_call", "os", "getpid", [], {})
        pid2 = svc2._request("function_call", "os", "getpid", [], {})

        self.assertNotEqual(pid1, pid2)


if __name__ == '__main__':
    unittest.main()
