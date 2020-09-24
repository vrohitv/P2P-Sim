from peer import BaseService, Connection
from messages import BaseMessage
class Ping(BaseMessage):
    pass
class Pong(BaseMessage):
    pass
class RequestPeers(BaseMessage):
    pass
class Hello(BaseMessage):
    "Offer a peer to connect"
    pass
class PeerList(BaseMessage):
    def __init__(self, sender, peers):
        self.sender = sender
        self.data = set(peers)
class PingHandler(BaseService):
    def handle_message(self, peer, msg):
        if isinstance(msg, Ping):
            peer.send(msg.sender, Pong(peer))
class PeerRequestHandler(BaseService):
    def handle_message(self, peer, msg):
        if isinstance(msg, RequestPeers):
            reply = PeerList(peer, peer.connections.keys())
            peer.send(msg.sender, reply)
class ConnectionManager(BaseService):
    max_silence_before_disconnect = 2
    ping_interval = 1
    max_peers = 10
    min_peers = 5
    min_keep_time = 5
    def __init__(self, peer):
        self.peer = peer
        self.last_seen = dict() # peer -> timestamp
        self.env.process(self.run())
        self.known_peers = set()
        self.disconnected_peers = set()
        def disconnect_cb(peer, other):
            assert peer == self.peer
            self.disconnected_peers.add(other)
        self.peer.disconnect_callbacks.append(disconnect_cb)
    def __repr__(self):
        return "ConnectionManager(%s)" % self.peer.name
    @property
    def env(self):
        return self.peer.env
    def handle_message(self, peer, msg):
        self.last_seen[msg.sender] = self.env.now
        if isinstance(msg, Hello):
            self.recv_hello(msg)
        if isinstance(msg, PeerList):
            self.recv_peerlist(msg)
    def ping_peers(self):
        for other in self.peer.connections:
            if self.env.now - self.last_seen.get(other, 0) > self.ping_interval:
                self.peer.send(other, Ping(sender=self.peer))
    def recv_hello(self, msg):
        other = msg.sender
        if not other in self.peer.connections:
            self.peer.connect(other)
            self.peer.send(other, Hello(self.peer))
            self.peer.send(other, RequestPeers(self.peer))
    def recv_peerlist(self, msg):
        peers = msg.data
        peers.discard(self.peer)
        self.known_peers.update(peers)
    def connect_peer(self, other):
        cnx = Connection(self.env, self.peer, other)
        cnx.send(Hello(self.peer), connect=True)
    def disconnect_unresponsive_peers(self):
        now = self.env.now
        for other in self.peer.connections.keys():
            if not other in self.last_seen:
                self.last_seen[other] = now
            elif now - self.last_seen[other] > self.max_silence_before_disconnect:
                print (self.peer, 'has not heard of', other, '...disconnecting')
                self.peer.disconnect(other)
    @property
    def connected_peers(self):
        return self.peer.connections.keys()
    @property
    def peer_candidates(self):
        candidates = self.known_peers.difference(set(self.connected_peers))
        return candidates.difference(self.disconnected_peers)
    def disconnect_slowest_peer(self):
        bw = lambda other: self.peer.connections[other].bandwidth
        if self.connected_peers:
            # get worst peer (based on latency)
            sorted_peers = [(bw(p), p) for p in self.connected_peers
                                    if p not in self.disconnected_peers]
            def Sort_Tuple(tup):
                tup.sort(key=lambda x: x[0])
                return tup
            sorted_peers = Sort_Tuple(sorted_peers)
            for bw, other in sorted_peers:
                start_time = self.peer.connections[other].start_time
                if self.env.now - start_time > self.min_keep_time:
                    self.peer.disconnect(other)
                    self.disconnected_peers.add(other)
                    print(self, 'disconnected slowest', other)
                    break

    def monitor_connections(self):
        # CASE: too few peers
        if len(self.connected_peers) < self.min_peers:
            needed = self.min_peers - len(self.connected_peers)
            candidates = self.peer_candidates
            if len(candidates) < needed:
                self.peer.broadcast(RequestPeers(self.peer))
            for other in list(candidates)[:needed]:
                self.connect_peer(other)
        if len(self.connected_peers) > self.max_peers:
            print(self, 'too many connections', len(self.connected_peers))
            num = max(0, len(self.connected_peers) - self.max_peers)
            print(self, 'disconnecting', num)
            for i in range(num):
                self.disconnect_slowest_peer()
    def run(self):
        while True:
            self.ping_peers()
            self.disconnect_unresponsive_peers()
            self.monitor_connections()
            yield self.env.timeout(self.ping_interval)




