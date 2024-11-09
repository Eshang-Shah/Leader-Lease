import grpc
import raft_pb2 as raft_pb2
import raft_pb2_grpc as raft_pb2_grpc


cluster_info={
    0: "localhost:50051",
    1: "localhost:50052",
    2: "localhost:50053",
    3: "localhost:50054",
    4: "localhost:50055"
}

def run():
    defLeader = 0
    while True:
        print("Welcome to Client! \nActions:\n1. GET Request \n2. Set Request \n3. Exit")
        choice = int(input("Enter your choice: "))
        if choice == 1:
            command = input("Enter [key]: ")
            leader = input(f"Leader ID ({defLeader}): ") or defLeader
            leader = int(leader)
            if leader not in cluster_info:
                print(f"Leader ID {leader} does not exist in cluster_info")
                continue
            command = "GET " + command.strip()
            req = raft_pb2.ServeClientRequest(request=command)
            try:
                with grpc.insecure_channel(cluster_info[leader]) as channel:
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    response = stub.ServeClient(req)
                    defLeader = int(response.leaderId) if response.leaderId != "NULL" else -1
                    print(
                        f"Client received: Leader: {response.leaderId}   Status: {response.success}  Message: {response.response}")
            except grpc.RpcError:
                print(f"Failed to send request to Node {leader}")

        elif choice == 2:
            command = input("Enter [key] [value]: ")
            leader = input(f"Leader ID ({defLeader}): ") or defLeader
            leader = int(leader)
            if leader not in cluster_info:
                print(f"Leader ID {leader} does not exist in cluster_info")
                continue
            command = "SET " + command.strip()
            req = raft_pb2.ServeClientRequest(request=command)
            try:
                with grpc.insecure_channel(cluster_info[leader]) as channel:
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    response = stub.ServeClient(req)
                    defLeader = int(response.leaderId) if response.leaderId != "NULL" else -1
                    print(
                        f"Client received: Leader: {response.leaderId}   Status: {response.success}  Message: {response.response}")
            except grpc.RpcError:
                print(f"Failed to send request to Node {leader}")

        elif choice == 3:
            exit()

        else:
            print("Wrong choice. Try Again!")


run()