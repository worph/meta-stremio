"""Beacon v2 — local resource advertise / discover over UDP multicast
(239.255.99.1:9099).

Every node advertises resources tagged with capability strings
(``metamesh.core``, ``metamesh.service/meta-stremio``,
``metamesh.transport/nzb@1``) and keeps a live view of everyone else's. This
service uses it to locate meta-core (the ``metamesh.core`` resource carries its
/urls block) and to feed the nav menu, with no shared volume.

The normative spec is docs/project-architecture/beacon-v2.md.

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

PROTO = "beacon"
VERSION = 2
DEFAULT_GROUP = "239.255.99.1"
DEFAULT_PORT = 9099
DEFAULT_INTERVAL = 10.0
# A node unheard for interval * this is dropped (evaluated at read time).
LIVENESS_FACTOR = 3
READ_BUFFER = 8192
# Senders stay under this so nothing fragments.
MAX_DATAGRAM = 1400

TYPE_PROBE = "probe"
TYPE_ADVERTISE = "advertise"
TYPE_BYE = "bye"

CAP_CORE = "metamesh.core"
CAP_SERVICE_PREFIX = "metamesh.service/"
CAP_ANY_SERVICE = "metamesh.service/*"


def cap_matches(pattern: str, cap: str) -> bool:
    """Does capability ``cap`` satisfy ``pattern``?

    ``*`` matches everything; an ``@N`` on the pattern must equal the cap's
    contract (none ignores it); ``x/*`` matches any variant of ``x``; otherwise
    the bases must be equal.
    """
    if pattern == "*":
        return True
    pbase, _, pc = pattern.rpartition("@") if "@" in pattern else (pattern, "", None)
    cbase, _, cc = cap.rpartition("@") if "@" in cap else (cap, "", None)
    if pc is not None and cc != pc:
        return False
    if pbase.endswith("/*"):
        prefix = pbase[:-2]
        if not cbase.startswith(prefix):
            return False
        rest = cbase[len(prefix):]
        return rest.startswith("/") and len(rest) > 1
    return pbase == cbase


def resource_matches(resource: dict, pattern: str) -> bool:
    return any(cap_matches(pattern, c) for c in resource.get("caps") or [])


def resource_endpoint(resource: dict, name: str) -> Optional[str]:
    """The named endpoint as an absolute URL ('/…' joins onto endpoints.http)."""
    endpoints = resource.get("endpoints") or {}
    v = endpoints.get(name)
    if not v:
        return None
    if not v.startswith("/"):
        return v
    base = endpoints.get("http")
    return base.rstrip("/") + v if base else None


def parse_message(data: bytes) -> Optional[dict]:
    """Decode a datagram; None for anything that is not well-formed beacon v2
    (beacon v1, meta-discovery v1, other versions, junk)."""
    try:
        m = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(m, dict) or m.get("proto") != PROTO or m.get("v") != VERSION:
        return None
    t = m.get("type")
    if t == TYPE_PROBE:
        return m
    if t in (TYPE_ADVERTISE, TYPE_BYE):
        node = m.get("node")
        if not isinstance(node, dict) or not node.get("name") or not node.get("instance"):
            return None
        return m
    return None


def _neighbor_row(node: dict, resources: List[dict], addr: str, last_seen: float) -> dict:
    caps: List[str] = []
    base_url = None
    for r in resources:
        for c in r.get("caps") or []:
            if c not in caps:
                caps.append(c)
        if base_url is None and resource_matches(r, CAP_ANY_SERVICE):
            base_url = resource_endpoint(r, "ui")
    row = dict(node)
    row.update({"resources": resources, "caps": caps, "addr": addr, "lastSeen": last_seen})
    if base_url:
        row["baseUrl"] = base_url
    return row


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
    """One beacon v2 participant: advertises its resources, answers probes,
    and keeps a TTL map of every other node."""

    def __init__(
        self,
        name: str,
        instance: Optional[str] = None,
        version: str = "",
        group: Optional[str] = None,
        port: Optional[int] = None,
        interval: Optional[float] = None,
        enabled: bool = True,
        payload: Optional[Callable[[], dict]] = None,
    ) -> None:
        self.name = name
        self.instance = instance or socket.gethostname()
        self.version = version
        self.group = group or os.environ.get("BEACON_GROUP") or DEFAULT_GROUP
        self.port = port or int(os.environ.get("BEACON_PORT") or DEFAULT_PORT)
        self.interval = interval or (float(os.environ.get("BEACON_INTERVAL_MS") or 0) / 1000.0) or DEFAULT_INTERVAL
        self.enabled = enabled
        # payload() -> {"status": str?, "resources": [resource, ...]}
        self._payload = payload

        self._sock: Optional[socket.socket] = None
        self._ifaces: List[tuple] = []
        self._nodes: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._running = False
        self._threads: List[threading.Thread] = []
        self._listeners: List[Callable[[dict], None]] = []

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._running or not self.enabled:
            if not self.enabled:
                print("[beacon] Disabled by configuration")
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
                print(f"[beacon] Warning: join {self.group} on {ifname} failed: {e}")

        self._sock = sock
        self._running = True
        print(
            f"[beacon] Listening on {self.group}:{self.port} as "
            f"{self.name}/{self.instance} (joined {joined} of {len(self._ifaces)} interfaces)"
        )

        for target in (self._read_loop, self._advertise_loop):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        # Bye, so neighbours drop us immediately rather than waiting out the
        # staleness window.
        try:
            self._send({"proto": PROTO, "v": VERSION, "type": TYPE_BYE, "node": self._node_info()})
        except OSError:
            pass
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None

    # -- api ---------------------------------------------------------------

    def probe(self, want: Optional[List[str]] = None) -> None:
        """Multicast a probe. Every node owning a resource matching one of
        ``want`` (every node, when empty) replies immediately."""
        msg = {"proto": PROTO, "v": VERSION, "type": TYPE_PROBE, "from": self.instance}
        if want:
            msg["want"] = list(want)
        self._send(msg)

    def neighbors(self) -> List[dict]:
        """Every live node, sorted by (name, instance)."""
        cutoff = time.time() - self.interval * LIVENESS_FACTOR
        with self._lock:
            for key in [k for k, v in self._nodes.items() if v["lastSeen"] < cutoff]:
                del self._nodes[key]
            out = [_neighbor_row(v["node"], v["resources"], v["addr"], v["lastSeen"]) for v in self._nodes.values()]
        return sorted(out, key=lambda n: (n.get("name") or "", n.get("instance") or ""))

    def neighbors_by_name(self) -> List[dict]:
        """One row per node name, most recently seen instance wins."""
        best: Dict[str, dict] = {}
        for nb in self.neighbors():
            cur = best.get(nb.get("name") or "")
            if cur is None or nb["lastSeen"] > cur["lastSeen"]:
                best[nb.get("name") or ""] = nb
        return sorted(best.values(), key=lambda n: n.get("name") or "")

    def self_announce(self) -> dict:
        """This node as a row, so a UI can render itself without waiting."""
        status, resources = self._get_payload()
        return _neighbor_row(self._node_info(status), resources, "", time.time())

    def on_advertise(self, cb: Callable[[dict], None]) -> None:
        self._listeners.append(cb)

    # -- internals ---------------------------------------------------------

    def _get_payload(self) -> tuple:
        if not self._payload:
            return None, []
        try:
            p = self._payload() or {}
        except Exception:
            p = {}
        return p.get("status"), list(p.get("resources") or [])

    def _node_info(self, status: Optional[str] = None) -> dict:
        n = {"name": self.name, "instance": self.instance}
        if self.version:
            n["version"] = self.version
        if status:
            n["status"] = status
        return n

    def _advertise_msg(self) -> dict:
        status, resources = self._get_payload()
        return {
            "proto": PROTO, "v": VERSION, "type": TYPE_ADVERTISE,
            "node": self._node_info(status), "resources": resources,
        }

    def _send(self, msg: dict, dest: Optional[tuple] = None) -> None:
        sock = self._sock
        if sock is None:
            return
        body = json.dumps(msg).encode("utf-8")
        if len(body) > MAX_DATAGRAM:
            print(f"[beacon] Warning: {len(body)}-byte datagram exceeds the fragmentation-safe {MAX_DATAGRAM}")
        with self._send_lock:
            if dest is not None:
                try:
                    sock.sendto(body, dest)
                except OSError as e:
                    print(f"[beacon] Reply to {dest} failed: {e}")
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
                    print(f"[beacon] Warning: send on {ifname} failed: {e}")

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
            msg = parse_message(data)
            if msg is None:
                continue  # v1 or foreign traffic on the shared group

            if msg["type"] == TYPE_PROBE:
                if msg.get("from") == self.instance:
                    continue
                ad = self._advertise_msg()
                want = msg.get("want") or []
                if not want or any(resource_matches(r, p) for r in ad["resources"] for p in want):
                    self._send(ad, dest=addr)
                continue

            node = msg["node"]
            if node["instance"] == self.instance:
                continue  # our own multicast echo
            if msg["type"] == TYPE_BYE:
                with self._lock:
                    self._nodes.pop(node["instance"], None)
                continue

            # Source address comes from the packet, never the payload.
            entry = {"node": node, "resources": msg.get("resources") or [], "addr": addr[0], "lastSeen": time.time()}
            with self._lock:
                self._nodes[node["instance"]] = entry
            row = _neighbor_row(entry["node"], entry["resources"], entry["addr"], entry["lastSeen"])
            for cb in list(self._listeners):
                try:
                    cb(row)
                except Exception as e:
                    print(f"[beacon] Listener raised: {e}")

    def _advertise_loop(self) -> None:
        self._send(self._advertise_msg())
        self.probe()
        while self._running:
            time.sleep(self.interval)
            if not self._running:
                return
            try:
                self._send(self._advertise_msg())
            except OSError:
                pass


class MetaCoreLocator:
    """Locates meta-core over beacon v2 (the first ``metamesh.core`` resource
    carrying ``data.urls``) and advertises this service as
    ``metamesh.service/<service_name>``."""

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
            version=version,
            enabled=_env_flag("ENABLE_UDP_DISCOVERY", True) if enabled is None else enabled,
            payload=lambda: {"resources": [{
                "id": "service",
                "caps": [CAP_SERVICE_PREFIX + service_name],
                "endpoints": {"ui": base_url or f"http://{local_ipv4()}"},
            }]},
        )
        self._node.on_advertise(self._on_advertise)

    def _on_advertise(self, nb: dict) -> None:
        if self._pin:
            return  # pinned: the wire cannot move us
        core = next((r for r in nb.get("resources") or [] if resource_matches(r, CAP_CORE)), None)
        urls = ((core or {}).get("data") or {}).get("urls")
        if not isinstance(urls, dict) or not urls.get("apiUrl"):
            return

        prev = self._current
        if prev is None:
            self._current = urls
            print(f"[beacon] meta-core discovered at {urls['apiUrl']} ({nb.get('addr')})")
            for cb in list(self._change_callbacks):
                try:
                    cb()
                except Exception as e:
                    print(f"[beacon] Change callback raised: {e}")
            return
        if prev.get("apiUrl") == urls.get("apiUrl"):
            self._current = urls
            return
        # A second, different core. Do not flap — keep the first and say so,
        # because on a PCS box this means a client container can see a core it
        # must not use.
        print(
            f"[beacon] Ignoring a second meta-core at {urls['apiUrl']} "
            f"({nb.get('addr')}); staying with {prev.get('apiUrl')}. "
            f"Set META_CORE_URL to pin this explicitly."
        )

    def start(self) -> None:
        if self._pin:
            print(f"[beacon] meta-core pinned to {self._pin}; discovery is advisory")
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
        an advertise interval.
        """
        deadline = None if timeout is None else time.time() + timeout
        logged = False
        while deadline is None or time.time() < deadline:
            url = self.get_api_url()
            if url:
                return url
            if not logged:
                print("[beacon] Waiting for meta-core...")
                logged = True
            self._node.probe([CAP_CORE])
            time.sleep(0.5)
        return None

    def on_change(self, cb: Callable[[], None]) -> None:
        self._change_callbacks.append(cb)

    def neighbors(self, all: bool = False, cap: Optional[str] = None) -> List[dict]:
        """One row per name (every instance with ``all``), optionally only
        nodes with a resource matching ``cap``."""
        rows = self._node.neighbors() if all else self._node.neighbors_by_name()
        if cap:
            rows = [r for r in rows if any(resource_matches(res, cap) for res in r.get("resources") or [])]
        return rows

    def self_announce(self) -> dict:
        return self._node.self_announce()

    def probe(self, want: Optional[List[str]] = None) -> None:
        self._node.probe(want)
