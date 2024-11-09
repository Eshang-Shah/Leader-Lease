
# raft_node.py
import time
from datetime import datetime
import threading
from concurrent import futures
import grpc
import raft_pb2 as raft_pb2
import raft_pb2_grpc as raft_pb2_grpc
import sys
import os
import json
import random
import numpy as np

sys.path.append('/proto')


cluster_info = {
    0: "localhost:50051",
    1: "localhost:50052",
    2: "localhost:50053",
    3: "localhost:50054",
    4: "localhost:50055"
}


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, total_nodes):
        self.node_id = node_id
        self.currentTerm = 0
        self.voted_for = None
        self.commitLen = 0
        self.sentLen = np.zeros(total_nodes, dtype=int)
        self.ackedLen = np.zeros(total_nodes, dtype=int)
        self.leaseDur = -1
        self.leaseInit = -1
        self.log = []
        self.data = {}
        self.state = "follower"  # Possible states: follower, candidate, leader
        self.leader_id = -1
        self.heartbeat_interval = 1.0
        # Start a background thread to trigger elections
        self.election_timeout = 10.0  # Example timeout in seconds
        self.grpc_timeout = 1
        self.election_timer_thread = None
        self.load_state()  # Load state from disk on startup
        self.start_election_timer()
        # threading.Thread(target=self.election_timer, daemon=True).start()

    def RequestVote(self, request, context):
        with threading.Lock():
            vote_granted = False
            leaseRem = 0.0
            #persistance check to get term number
            last_log_index, last_log_term = self.get_last_log_info()
            print(
                f"Node {self.node_id}: Received VoteRequest from Node {request.candidateId} for term {request.term}.")
            if self.currentTerm < request.term:
                self.currentTerm = request.term
                if self.state == 'leader':
                    message = f"Leader {self.node_id} Stepping Down."
                    self.appendDump(message)
                    print(message)
                self.state = "follower"
                self.voted_for = None
            logOk = (request.lastLogTerm > last_log_term) or (
                (request.lastLogTerm == last_log_term) and (request.lastLogIndex >= last_log_index))
            if request.term == self.currentTerm and logOk and self.voted_for in [None, request.candidateId]:
                self.voted_for = request.candidateId
                # self.save_state()
                vote_granted = True
                if self.leaseDur != -1:
                    leaseRem = max(0.0, self.leaseDur -
                                   (time.time() - self.leaseInit))

                message = f"Node {self.node_id}: Granted Vote to Node {request.candidateId} for Term {request.term}."
                self.appendDump(message)
                print(message)
                self.reset_election_timer()
            else:
                message = f"Node {self.node_id}: Denied Vote to Node {request.candidateId} for Term {request.term}."
                self.appendDump(message)
                print(message)
            self.updateMetadata()
            return raft_pb2.VoteReply(term=self.currentTerm, voteGranted=vote_granted, leaseRem=leaseRem)

    def get_last_log_info(self):
        term = 0
        index = 0
        if len(self.log) > 0:
            index = len(self.log) - 1
            term = self.log[-1]['term']
        return (index, term)

    def AppendEntries(self, request, context):
        with threading.Lock():
            success = False
            ack = 0
            # Check if the received term is at least as large as the node's current term
            if request.term > self.currentTerm:
                self.currentTerm = request.term
                self.voted_for = None
                # Reset the election timer upon receiving a heartbeat
                # self.reset_election_timer()
            if request.term == self.currentTerm:
                if self.state == 'leader':
                    message = f"Leader {self.node_id} Stepping Down."
                    self.appendDump(message)
                    print(message)
                self.state = "follower"
                self.leader_id = request.leaderId
            logOk = (len(self.log) >= request.prevLogIndex) and ((request.prevLogIndex == 0) or (
                self.log[request.prevLogIndex - 1]['term'] == request.prevLogTerm))
            if request.term == self.currentTerm and logOk:
                success = True
                self.appendEntries2(request.prevLogIndex,
                                    request.leaderCommit, request.entries)
                ack = request.prevLogIndex + len(request.entries)
                self.saveLog()
                self.print_log()
                self.leaseDur = request.leaseDuration
                self.leaseInit = time.time()
                message = f"Node {self.node_id} accepted AppendEntries RPC from {self.leader_id}."
                self.appendDump(message)
                print(message)
                self.reset_election_timer()
            else:
                message = f"Node {self.node_id} rejected AppendEntries RPC from {self.leader_id}."
                self.appendDump(message)
                print(message)
            self.updateMetadata()
            return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=success, ack=ack)

    def print_log(self):
        # print(f"Updated Log for Node {self.node_id}: ")
        for i, entry in enumerate(self.log):
            print(f"{i}: Term: {entry['term']}    Command: {entry['command']}")

    def appendEntries2(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and (len(self.log) > prefixLen):
            index = min(len(self.log), prefixLen + len(suffix)) - 1
            if self.log[index]['term'] != suffix[index - prefixLen].term:
                log2 = []
                for i in range(prefixLen):
                    log2.append(self.log[i])
                self.log = log2
        if (prefixLen + len(suffix)) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(suffix)):
                self.log.append(
                    {"term": suffix[i].term, "command": suffix[i].command})
        if leaderCommit > self.commitLen:
            for i in range(self.commitLen, leaderCommit):
                self.deliver(self.log[i]['command'])
                message = f"Node {self.node_id} (follower) committed the entry {self.log[i]['command']} to the state machine."
                self.appendDump(message)
                print(message)
            self.commitLen = leaderCommit

    def ServeClient(self, request, context):
        print(f"Node {self.node_id} received request: {request.request}")
        if self.state != "leader":
            return raft_pb2.ServeClientReply(response="Node not a Leader!!", leaderId=str(self.leader_id) if self.leader_id not in [self.node_id, -1] else "NULL", success=False)
        # Handle SET operation
        message = f"Node {self.leader_id} (leader) received a {request.request} request."
        self.appendDump(message)
        print(message)
        command = request.request.split()[0]
        success = False
        response = "Did not get majority acks!!"
        if command == "SET":
            key, value = request.request.split()[1], request.request.split()[2]

            self.log.append({"command": request.request,
                            "term": self.currentTerm})
            self.saveLog()
            print(
                f"Node {self.node_id} appended request: {request.request} with term: {self.currentTerm}")
            # self.save_state()
            self.ackedLen[self.node_id] = len(self.log)
            if self.broadcast_message():  # Have to add a more robust check for committed command
                success = True
                response = "Command Executed!!"
            self.updateMetadata()
        elif command == "GET":
            if self.leaseDur != -1 and (self.leaseDur - (time.time() - self.leaseInit)) > 0:
                if request.request.split()[1] in self.data.keys():
                    success = True
                    response = f"Value of {request.request.split()[1]}: {self.data[request.request.split()[1]]}."
                else:
                    success = False
                    response = ""
            else:
                success = False
                response = f"Lease Duration Expired for leader ID {self.leader_id}."
        return raft_pb2.ServeClientReply(response=response, leaderId=str(self.node_id), success=success)

    def reset_election_timer(self):
        """Resets the election timeout timer."""
        if self.election_timer_thread:
            self.election_timer_thread.cancel()
        self.start_election_timer()

    def start_election_timer(self):
        """Starts or restarts the election timeout timer."""
        self.election_timeout = random.uniform(5, 10)
        print(f"Election Timeout: {self.election_timeout}")
        # self.election_timeout = random.uniform(150, 300) / 1000.0  # Example: 150ms to 300ms
        self.election_timer_thread = threading.Timer(
            self.election_timeout, self.initiate_election)
        self.election_timer_thread.daemon = True
        self.election_timer_thread.start()

    def initiate_election(self):
        with threading.Lock():
            # Check if the node is already a leader, to prevent unnecessary elections
            if self.state == "leader":
                print(
                    f"Node {self.node_id} is already a leader. Skipping election.")
                self.reset_election_timer()
                return

            self.state = "candidate"
            self.currentTerm += 1
            self.leader_id = -1
            message = f"Node {self.node_id} election timer timed out, Starting election for term {self.currentTerm}."
            self.appendDump(message)
            print(message)
            self.voted_for = self.node_id
            # self.save_state()
            vote_count = 1  # Starts with voting for itself
            leasetimes = np.array([0]*len(self.ackedLen), dtype=float)
            self.updateMetadata()
            for peer_id, peer_address in cluster_info.items():
                if peer_id == self.node_id:
                    continue
                try:
                    channel = grpc.insecure_channel(peer_address)
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    response = stub.RequestVote(raft_pb2.VoteRequest(
                        term=self.currentTerm,
                        candidateId=self.node_id,
                        lastLogIndex=self.get_last_log_info()[0],
                        lastLogTerm=self.get_last_log_info()[1],
                    ), timeout=self.grpc_timeout)
                    if self.state == "candidate" and response.voteGranted and (self.currentTerm == response.term):
                        vote_count += 1
                        leasetimes[peer_id] = response.leaseRem
                        print(
                            f"Node {peer_id} voted for Node {self.node_id} for term {self.currentTerm}.")
                    elif response.term > self.currentTerm:
                        self.currentTerm = response.term
                        self.state = "follower"
                        self.voted_for = None
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        print(
                            f"RequestVote RPC call timed out for node {peer_id}")
                    else:
                        print(
                            f"RequestVote RPC call for node {peer_id} failed with error: {e}")
                finally:
                    channel.close()

            if (vote_count > len(cluster_info) / 2) and self.state != "follower":
                # self.become_leader()
                self.state = "leader"
                self.leader_id = self.node_id
                message = f"Node {self.node_id} became the leader for term {self.currentTerm}."
                self.appendDump(message)
                print(message)
                if self.leaseDur != -1:
                    leasetimes[self.node_id] = max(
                        0.0, self.leaseDur - (time.time() - self.leaseInit))
                message = f"New Leader {self.leader_id} waiting for Old Leader Lease to timeout: {leasetimes.max()}s"
                self.appendDump(message)
                print(message)
                time.sleep(leasetimes.max())
                self.leaseDur = 10
                self.log.append({"term": self.currentTerm, "command": "NO-OP"})
                self.saveLog()
                print(
                    f"Node {self.node_id} becomes leader with {vote_count} votes and appends NO_OP to its log.")
                self.ackedLen[self.node_id] = len(self.log)
                for peer_id, peer_address in cluster_info.items():
                    if peer_id != self.node_id:
                        self.sentLen[peer_id] = len(self.log) - 1
                        self.ackedLen[peer_id] = 0
                        # self.replicateLog(peer_id, peer_address)
                self.updateMetadata()
                self.send_heartbeats()

            else:
                # Remain or revert to follower state if not enough votes are received.
                self.state = "follower"
                print(
                    f"Node {self.node_id} did not receive enough votes and remains or reverts to a follower.")
                self.updateMetadata()

            # Reset the election timer to wait for the next election cycle.
            self.reset_election_timer()

    def replicateLog(self, id, address):
        with threading.Lock():
            returnval = False
            prefixLen = self.sentLen[id]
            entries = []
            for i in range(prefixLen, len(self.log)):
                entry = raft_pb2.LogEntry(
                    term=self.log[i]["term"], command=self.log[i]["command"])
                entries.append(entry)
            prefixTerm = 0
            if prefixLen > 0:
                prefixTerm = self.log[prefixLen-1]["term"]
            req = raft_pb2.AppendEntriesRequest()
            req.entries.extend(entries)
            req.term = self.currentTerm
            req.leaderId = self.node_id
            req.prevLogIndex = prefixLen
            req.prevLogTerm = prefixTerm
            req.leaseDuration = self.leaseDur
            req.leaderCommit = self.commitLen
            try:
                res = None
                with grpc.insecure_channel(address) as channel:
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    res = stub.AppendEntries(req, timeout=self.grpc_timeout)
                print(
                    f"Sent AppendEntries Request to Node ID: {id} with response: {res.success}.")
                returnval = res.success
                if res.term == self.currentTerm and self.state == 'leader':
                    if res.success and res.ack > self.ackedLen[id]:
                        self.sentLen[id] = res.ack
                        self.ackedLen[id] = res.ack
                        self.commitLog()
                    elif self.sentLen[id] > 0:
                        self.sentLen[id] -= 1
                        returnval = self.replicateLog(id, address)
                elif res.term > self.currentTerm:
                    self.currentTerm = res.term
                    if self.state == 'leader':
                        message = f"Leader {self.node_id} Stepping Down."
                        self.appendDump(message)
                        print(message)
                    self.state = "follower"
                    self.voted_for = None
                    self.reset_election_timer()
            except grpc.RpcError as e:
                # print(f"Failed to send AppendEntries Request to Node ID: {id}. Error: {e}")
                message = f"Error occurred while sending RPC to Node {id}."
                self.appendDump(message)
                print(message)
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print(f"AppendEntries RPC call timed out for node {id}")
                else:
                    print(
                        f"AppendEntries RPC call for node {id} failed with error: {e}")
            return returnval

    def commitLog(self):
        quorum = int((len(self.ackedLen))/2)
        minacks = quorum + 1
        ready = np.arange(np.sort(self.ackedLen)[
                          ::-1][quorum] + 1, dtype=int)[1:]
        if len(ready) > 0 and (ready.max() > self.commitLen) and (self.log[ready.max()-1]['term'] == self.currentTerm):
            for i in range(self.commitLen, ready.max()):
                self.deliver(self.log[i]['command'])
                message = f"Node {self.node_id} (leader) committed the entry {self.log[i]['command']} to the state machine."
                self.appendDump(message)
                print(message)
            self.commitLen = ready.max()

    def deliver(self, command):
        with threading.Lock():
            command = command.split()
            if len(command) == 3 and command[0] == 'SET':
                self.data[command[1]] = command[2]

    def saveLog(self):
        with threading.Lock():
            with open(self.logs_path, "w") as f:
                for entry in self.log:
                    f.write(f"{entry['command']} {entry['term']}\n")

    def appendDump(self, message):
        with threading.Lock():
            with open(self.dump_path, "a") as f:
                f.write(f"{message}\n")

    def updateMetadata(self):
        with threading.Lock():
            metaData = {'id': int(self.node_id), 'currTerm': int(self.currentTerm), 'voted_for': self.voted_for, 'commitLen': int(self.commitLen),
                        'sentLen': self.sentLen.tolist(), 'ackedLen': self.ackedLen.tolist(), 'leaseDur': int(self.leaseDur), 'leaseInit': int(self.leaseInit),
                        'state': self.state, 'leaderID': int(self.leader_id), }
            with open(self.metadata_path, "w") as outfile:
                json.dump(metaData, outfile)

    def send_heartbeats(self):
        if self.state == "leader":
            currtime = datetime.now().time().strftime('%H:%M:%S')
            message = f"Leader {self.leader_id} sending heartbeat & Renewing Lease."
            self.appendDump(message)
            print(f"{currtime}: {message}")
            self.leaseDur = 10
            leaseinit = time.time()
            cnt = 1
            for peer_id, peer_address in cluster_info.items():
                if peer_id != self.node_id and self.state == 'leader':
                    if self.replicateLog(peer_id, peer_address):
                        cnt += 1
            if cnt > len(self.ackedLen)/2:
                self.leaseInit = leaseinit
            elif (self.leaseDur - (time.time() - self.leaseInit)) < 0:
                message = f"Leader {self.leader_id} lease renewal failed. Stepping Down."
                self.appendDump(message)
                print(message)
                self.state = "follower"
            self.updateMetadata()
            # Schedule the next heartbeat
            threading.Timer(self.heartbeat_interval,
                            self.send_heartbeats).start()

    def broadcast_message(self):
        if self.state == "leader":
            currtime = datetime.now().time().strftime('%H:%M:%S')
            print(f"{currtime}: Broadcasting Message from Node: {self.node_id}")
            self.leaseDur = 10
            leaseinit = time.time()
            cnt = 1
            for peer_id, peer_address in cluster_info.items():
                if peer_id != self.node_id and self.state == 'leader':
                    if self.replicateLog(peer_id, peer_address):
                        cnt += 1
            if cnt > len(self.ackedLen)/2:
                self.leaseInit = leaseinit
                return True
            elif (self.leaseDur - (time.time() - self.leaseInit)) < 0:
                message = f"Leader {self.leader_id} lease renewal failed. Stepping Down."
                self.appendDump(message)
                print(message)
                self.state = "follower"
        return False

    def load_state(self):

        self.directory = f"logs_node_{self.node_id}"
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        self.metadata_path = os.path.join(self.directory, "metadata.json")
        self.logs_path = os.path.join(self.directory, "logs.txt")
        self.dump_path = os.path.join(self.directory, "dump.txt")
       
        # Check if the metadata file exists and is not empty
        if os.path.isfile(self.metadata_path) and os.path.getsize(self.metadata_path) > 0:
            try:
                with open(self.metadata_path, "r") as file:
                    metadata = json.load(file)
                    # self.metaData = {'id': self.node_id, 'currTerm': self.currentTerm, 'voted_for': self.voted_for, 'commitLen': self.commitLen,
                    #      'sentLen': self.sentLen, 'ackedLen': self.ackedLen, 'leaseDur': self.leaseDur, 'leaseInit':self.leaseInit,
                    #      'state': self.state, 'leaderID': self.leader_id, }
                    self.currentTerm = metadata.get("currTerm", 0)
                    self.voted_for = metadata.get("voted_for", None)
                    self.commitLen = metadata.get("commitLen", 0)
                    self.node_id = metadata.get("id", self.node_id)
                    self.sentLen = np.array(metadata.get(
                        "sentLen", self.sentLen), dtype=int)
                    self.ackedLen = np.array(metadata.get(
                        "ackedLen", self.ackedLen), dtype=int)
                    self.leaseDur = metadata.get("leaseDur", self.leaseDur)
                    self.leaseInit = metadata.get("leaseInit", self.leaseInit)
                    self.state = metadata.get("state", self.state)
                    self.leader_id = metadata.get("leaderID", self.leader_id)
                print("Successfully loaded metaData!!")
                # print(self.metaData)
            except json.JSONDecodeError:
                print(
                    f"Error decoding JSON from {self.metadata_path}. Initializing with default values.")
        else:
            print(
                f"No existing metadata found or file is empty for node {self.node_id}. Initializing with default values.")

        # Attempt to load logs if the logs file exists
        if os.path.isfile(self.logs_path) and os.path.getsize(self.logs_path) > 0:
            try:
                with open(self.logs_path, "r") as file:
                    lines = file.readlines()
                    for line in lines:
                        self.log.append(
                            {"term": int(line.split()[-1]), "command": " ".join(line.split()[:-1]).strip()})
                    # self.log = [{"command": line.split()[:-1][0], "term": int(
                    #     line.split()[-1])} for line in file if line.strip()]
                print("Successfully loaded Logs!!")
                self.print_log()
            except Exception as e:
                print(f"Error reading log entries from {self.logs_path}: {e}")
        else:
            self.log = []

        for entry in self.log[:self.commitLen]:
            if len(entry['command'].split()) == 3:
                key = entry['command'].split()[1]
                value = entry['command'].split()[2]
                self.data[key] = value


def serve(node_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_service = RaftNode(node_id, len(cluster_info))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_service, server)
    # Use node_id to lookup the address in the globally defined cluster_info
    server.add_insecure_port("0.0.0.0:50055")
    server.start()
    print(f"Node {node_id} listening on {cluster_info[node_id]}")
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python raft_node.py <node_id>")
        sys.exit(1)
    node_id = int(sys.argv[1])
    serve(node_id)