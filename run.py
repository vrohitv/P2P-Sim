import random
import simpy
from peer import  Peer
from peermanager import ConnectionManager
from peermanager import PingHandler
from peermanager import PeerRequestHandler
from disruptions import Downtime
from disruptions import Slowdown
NUM_PEERS = int(input("Enter the number of Peers:"))
SIM_DURATION = 30
KBit = 1024/8
VISUALIZATION = True
def managed_peer(name, env):
    p = Peer(name, env)
    p.services.append(ConnectionManager(p))
    p.services.append(PeerRequestHandler())
    p.services.append(PingHandler())
    p.services.append(Downtime(env, p))
    p.services.append(Slowdown(env, p))
    return p
def create_peers(peerserver, num):
    peers = []
    for i in range(num):
        p = managed_peer('P%d' % i, env)
        connection_manager = p.services[0]
        connection_manager.connect_peer(peerserver)
        peers.append(p)
    for p in peers[:int(num * 0.5)]:
        p.bandwidth_ul = max(384, random.gauss(12000, 6000)) * KBit
        p.bandwidth_dl = max(128, random.gauss(4800, 2400)) * KBit
    for p in peers[int(num * 0.5):]:
        p.bandwidth_dl = p.bandwidth_ul = max(10000, random.gauss(100000, 50000)) * KBit
    return peers
env = simpy.Environment()
pserver = managed_peer('PeerServer', env)
pserver.bandwidth_ul = pserver.bandwidth_dl = 128 * KBit
peers = create_peers(pserver, NUM_PEERS)
print('starting sim')
if VISUALIZATION:
    from visualize import Visualizer
    Visualizer(env, peers)
else:
    env.run(until=SIM_DURATION)
