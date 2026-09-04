import socket
import sys

"""End-to-end framework call test over the demo protocol.

Commands exercised:
    fwbind    bind current connection to virtual_fd/group
    fwgroup   group broadcast
    fwsend    single send
    fwdivide  rebuild a connection group
    fwclose   request close of a target connection
"""

HOST = "127.0.0.1"
PORT = 9001


def frame(payload):
    return ("%04d" % len(payload) + payload).encode()


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def recv_frame(sock):
    header = recv_exact(sock, 4)
    if header is None:
        return None
    body = recv_exact(sock, int(header))
    return None if body is None else body.decode()


def send(sock, payload):
    sock.sendall(frame(payload))


def recv_ok(sock, want):
    got = recv_frame(sock)
    if got != want:
        raise AssertionError("want %r, got %r" % (want, got))


def recv_any(sock, wants):
    got = recv_frame(sock)
    if got not in wants:
        raise AssertionError("want one of %r, got %r" % (wants, got))
    return got


def main():
    a = socket.create_connection((HOST, PORT), timeout=5)
    b = socket.create_connection((HOST, PORT), timeout=5)
    for s in (a, b):
        s.settimeout(5)

    send(a, "fwbind:1001:7")
    recv_ok(a, "fwbind-ok")
    send(b, "fwbind:2002:7")
    recv_ok(b, "fwbind-ok")

    send(a, "fwgroup:7:hello")
    recv_any(a, {"fw:group:hello", "fwgroup-ok"})
    recv_any(a, {"fw:group:hello", "fwgroup-ok"})
    recv_ok(b, "fw:group:hello")

    send(a, "fwsend:2002:single")
    recv_ok(a, "fwsend-ok")
    recv_ok(b, "fw:to:single")

    send(a, "fwdivide:8:1001,2002")
    recv_ok(a, "fwdivide-ok")

    send(a, "fwgroup:8:hello8")
    recv_any(a, {"fw:group:hello8", "fwgroup-ok"})
    recv_any(a, {"fw:group:hello8", "fwgroup-ok"})
    recv_ok(b, "fw:group:hello8")

    send(a, "fwclose:2002")
    recv_ok(a, "fwclose-ok")
    if recv_frame(b) is not None:
        raise AssertionError("target connection should be closed")

    b.close()
    b = socket.create_connection((HOST, PORT), timeout=5)
    b.settimeout(5)
    send(b, "fwbind:2002:8")
    recv_ok(b, "fwbind-ok")

    send(a, "fwgroup:8:rejoin")
    recv_any(a, {"fw:group:rejoin", "fwgroup-ok"})
    recv_any(a, {"fw:group:rejoin", "fwgroup-ok"})
    recv_ok(b, "fw:group:rejoin")

    a.close()
    b.close()
    print("FRAMEWORK_CALL_TEST-OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
