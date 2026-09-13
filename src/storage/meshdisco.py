"""meta-discovery v1 — UDP multicast service discovery.

Replaces the two file-based mechanisms that required a shared /meta-core
volume: ``locks/kv-leader.info`` (locating meta-core at boot) and
``services/<name>-<host>.json`` (the dashboard nav registry). Both answered the
same question — where is meta-core, and who else is on this network — so one
announce packet answers both, and discovery scope becomes the docker network
rather than the mounted volume.

The normative spec is docs/project-architecture/service-discovery.md.

Two details this module must get right, both of which fail silently:

1. **Per-interface fan-out.** A socket that joins with ``INADDR_ANY`` and sends
   via the default route covers exactly one interface. Containers here are
   routinely attached to two docker networks, so we enumerate interfaces and
   join/send on each using a hand-packed ``ip_mreqn``.
2. **The pin wins.** When ``META_CORE_URL`` is set, the wire is never consulted
   for core selection — otherwise a container that can see two cores could
   latch onto the wrong one.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import threading
import time
from typing import Callable, Dict, List, Optional

PROTOCOL_VERSION = 1
DEFAULT_GROUP = "239.255.77.1"
DEFAULT_PORT = 9399
DEFAULT_INTERVAL = 10.0
# A neighbour unseen for interval * this is dropped. Replaces the reaper.
LIVENESS_FACTOR = 3
READ_BUFFER = 8192

TYPE_DISCOVERY = "discovery"
TYPE_ANNOUNCE = "announce"
ROLE_CORE = "core"
ROLE_SERVICE = "service"


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() not in ("false", "0", "no", "off")


def _interface_indexes() -> List[tuple]:
    """(name, index, ipv4) for every up, non-loopback interface.

    Falls back to a single unspecified entry when the platform cannot
    enumerate, so discovery still works on the default route.
    """
    out: List[tuple] = []
    try:
        for idx, name in socket.if_nameindex():
            if name == "lo":
                continue
            try:
                # Cheap way to get the interface's IPv4 without netifaces.
                addr = _ipv4_of(name)
            except OSError:
                continue
            if addr:
                out.append((name, idx, addr))
    except (AttributeError, OSError):
        pass
    return out


def _ipv4_of(ifname: str) -> Optional[str]:
    """SIOCGIFADDR — avoids a netifaces dependency."""
    import fcntl

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        packed = struct.pack("256s", ifname[:15].encode("utf-8"))
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, packed)[20:24])
    except OSError:
        return None
    finally:
        s.close()


def local_ipv4() -> str:
    """First non-loopback IPv4, else the hostname. Matches the Go helper."""
    ifaces = _interface_indexes()
    if ifaces:
        return ifaces[0][2]
    return socket.gethostname()


class MeshNode:
    """Announces this service, answers probes, tracks neighbours."""

    def __init__(
        self,
        name: str,
        role: str = ROLE_SERVICE,
        instance: Optional[str] = None,
        version: str = "",
        group: str = DEFAULT_GROUP,
        port: int = DEFAULT_PORT,
        interval: float = DEFAULT_INTERVAL,
        enabled: bool = True,
        payload: Optional[Callable[[], dict]] = None,
    ) -> None:
        self.name = name
        self.role = role
        self.instance = instance or socket.gethostname()
        self.version = version
        self.group = group
        self.port = port
        self.interval = interval
        self.enabled = enabled
        self._payload = payload

        self._sock: Optional[socket.socket] = None
        self._ifaces: List[tuple] = []
        self._neighbors: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._running = False
        self._threads: List[threading.Thread] = []
        self._listeners: List[Callable[[dict], None]] = []

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._running or not self.enabled:
            if not self.enabled:
                print("[meshdisco] Disabled by configuration")
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        sock.bind(("0.0.0.0", self.port))
        # TTL 1: link-local only, never routed.
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)

        self._ifaces = _interface_indexes()
        group_bin = socket.inet_aton(self.group)
        joined = 0
        for ifname, idx, addr in self._ifaces:
            try:
                # ip_mreqn: multiaddr, address (INADDR_ANY), ifindex. The
                # ifindex is what makes this a per-interface join instead of
                # "whatever the routing table picks".
                mreq = group_bin + socket.inet_aton("0.0.0.0") + struct.pack("@i", idx)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                joined += 1
            except OSError as e:
                print(f"[meshdisco] Warning: join {self.group} on {ifname} failed: {e}")

        self._sock = sock
        self._running = True
        print(
            f"[meshdisco] Listening on {self.group}:{self.port} as "
            f"{self.name}/{self.instance} (joined {joined} of {len(self._ifaces)} interfaces)"
        )

        for target in (self._read_loop, self._announce_loop):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        # Final announce so neighbours drop us immediately rather than waiting
        # out the staleness window.
        try:
            self._send(self._build_announce("stopping"))
        except OSError:
            pass
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None

    # -- api ---------------------------------------------------------------

    def probe(self) -> None:
        """Multicast a probe; every listener replies immediately."""
        self._send({"v": PROTOCOL_VERSION, "type": TYPE_DISCOVERY})

    def neighbors(self) -> List[dict]:
        cutoff = time.time() - self.interval * LIVENESS_FACTOR
        with self._lock:
            for key in [k for k, v in self._neighbors.items() if v["lastSeen"] < cutoff]:
                del self._neighbors[key]
            out = list(self._neighbors.values())
        return sorted(out, key=lambda n: n.get("name") or "")

    def neighbors_by_name(self) -> List[dict]:
        best: Dict[str, dict] = {}
        for nb in self.neighbors():
            cur = best.get(nb.get("name") or "")
            if cur is None or nb["lastSeen"] > cur["lastSeen"]:
                best[nb.get("name") or ""] = nb
        return sorted(best.values(), key=lambda n: n.get("name") or "")

    def cores(self) -> List[dict]:
        return [n for n in self.neighbors() if n.get("role") == ROLE_CORE and n.get("urls")]

    def self_announce(self) -> dict:
        msg = self._build_announce("running")
        msg["addr"] = ""
        msg["lastSeen"] = time.time()
        return msg

    def on_announce(self, cb: Callable[[dict], None]) -> None:
        self._listeners.append(cb)

    # -- internals ---------------------------------------------------------

    def _build_announce(self, status: str) -> dict:
        msg = {
            "v": PROTOCOL_VERSION,
            "type": TYPE_ANNOUNCE,
            "name": self.name,
            "instance": self.instance,
            "role": self.role,
            "version": self.version,
            "status": status,
        }
        if self._payload:
            try:
                p = self._payload() or {}
            except Exception:
                p = {}
            if p.get("baseUrl"):
                msg["baseUrl"] = p["baseUrl"]
            if p.get("status") and status == "running":
                msg["status"] = p["status"]
            # Only a core may carry a URLs block.
            if self.role == ROLE_CORE and p.get("urls"):
                msg["urls"] = p["urls"]
        return msg

    def _send(self, msg: dict, dest: Optional[tuple] = None) -> None:
        sock = self._sock
        if sock is None:
            return
        body = json.dumps(msg).encode("utf-8")
        with self._send_lock:
            if dest is not None:
                try:
                    sock.sendto(body, dest)
                except OSError as e:
                    print(f"[meshdisco] Reply to {dest} failed: {e}")
                return
            # One write per interface — the default route alone reaches a
            # single network.
            for ifname, idx, addr in self._ifaces:
                try:
                    sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_IF,
                        socket.inet_aton(addr) + socket.inet_aton("0.0.0.0") + struct.pack("@i", idx),
                    )
                    sock.sendto(body, (self.group, self.port))
                except OSError as e:
                    print(f"[meshdisco] Warning: send on {ifname} failed: {e}")

    def _read_loop(self) -> None:
        while self._running:
            sock = self._sock
            if sock is None:
                return
            try:
                data, addr = sock.recvfrom(READ_BUFFER)
            except OSError:
                if not self._running:
                    return
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue  # foreign traffic on the group
            if msg.get("v") != PROTOCOL_VERSION:
                continue
            # Drop our own multicast echo.
            if msg.get("instance") == self.instance and msg.get("name") == self.name:
                continue

            if msg.get("type") == TYPE_DISCOVERY:
                self._send(self._build_announce("running"), dest=addr)
                continue
            if msg.get("type") != TYPE_ANNOUNCE or not msg.get("name"):
                continue

            key = f"{msg['name']}|{msg.get('instance', '')}"
            if msg.get("status") == "stopping":
                with self._lock:
                    self._neighbors.pop(key, None)
                continue

            # Source address comes from the packet, never the payload.
            msg["addr"] = addr[0]
            msg["lastSeen"] = time.time()
            with self._lock:
                self._neighbors[key] = msg
            for cb in list(self._listeners):
                try:
                    cb(msg)
                except Exception as e:
                    print(f"[meshdisco] Listener raised: {e}")

    def _announce_loop(self) -> None:
        self._send(self._build_announce("running"))
        self.probe()
        while self._running:
            time.sleep(self.interval)
            if not self._running:
                return
            try:
                self._send(self._build_announce("running"))
            except OSError:
                pass


class MetaCoreLocator:
    """Locates meta-core over UDP, replacing the kv-leader.info read."""

    def __init__(
        self,
        service_name: str,
        base_url: Optional[str] = None,
        version: str = "",
        meta_core_url: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> None:
        self._pin = (meta_core_url or "").strip() or None
        self._current: Optional[dict] = None
        self._change_callbacks: List[Callable[[], None]] = []

        self._node = MeshNode(
            name=service_name,
            role=ROLE_SERVICE,
            version=version,
            enabled=_env_flag("ENABLE_UDP_DISCOVERY", True) if enabled is None else enabled,
            payload=lambda: {"baseUrl": base_url or f"http://{local_ipv4()}"},
        )
        self._node.on_announce(self._on_announce)

    def _on_announce(self, nb: dict) -> None:
        if nb.get("role") != ROLE_CORE or not nb.get("urls"):
            return
        if self._pin:
            return  # pinned: the wire cannot move us

        prev = self._current
        if prev is None:
            self._current = nb["urls"]
            print(f"[meshdisco] meta-core discovered at {nb['urls']['apiUrl']} ({nb.get('addr')})")
            for cb in list(self._change_callbacks):
                try:
                    cb()
                except Exception as e:
                    print(f"[meshdisco] Change callback raised: {e}")
            return
        if prev.get("apiUrl") == nb["urls"].get("apiUrl"):
            self._current = nb["urls"]
            return
        # A second, different core. Do not flap — keep the first and say so,
        # because on a PCS box this means a client container can see a core it
        # must not use.
        print(
            f"[meshdisco] Ignoring a second meta-core at {nb['urls']['apiUrl']} "
            f"({nb.get('addr')}); staying with {prev.get('apiUrl')}. "
            f"Set META_CORE_URL to pin this explicitly."
        )

    def start(self) -> None:
        if self._pin:
            print(f"[meshdisco] meta-core pinned to {self._pin}; discovery is advisory")
        self._node.start()

    def stop(self) -> None:
        self._node.stop()
        self._change_callbacks = []

    def get_api_url(self) -> Optional[str]:
        if self._pin:
            return self._pin
        if self._current:
            return self._current.get("apiUrl")
        return None

    def get_urls(self) -> Optional[dict]:
        return self._current

    def wait_for_core(self, timeout: Optional[float] = 30.0) -> Optional[str]:
        """Block until meta-core is located.

        ``timeout=None`` waits forever, which is meta-stremio's default
        (LEADER_WAIT_TIMEOUT=0). Re-probes every 500ms rather than waiting out
        an announce interval.
        """
        deadline = None if timeout is None else time.time() + timeout
        logged = False
        while deadline is None or time.time() < deadline:
            url = self.get_api_url()
            if url:
                return url
            if not logged:
                print("[meshdisco] Waiting for meta-core...")
                logged = True
            self._node.probe()
            time.sleep(0.5)
        return None

    def on_change(self, cb: Callable[[], None]) -> None:
        self._change_callbacks.append(cb)

    def neighbors(self) -> List[dict]:
        return self._node.neighbors_by_name()

    def self_announce(self) -> dict:
        return self._node.self_announce()

    def probe(self) -> None:
        self._node.probe()
