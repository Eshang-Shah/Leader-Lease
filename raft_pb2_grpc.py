# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import raft_pb2 as raft__pb2


class RaftServiceStub(object):
    """The Raft service definition.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RequestVote = channel.unary_unary(
                '/raft.RaftService/RequestVote',
                request_serializer=raft__pb2.VoteRequest.SerializeToString,
                response_deserializer=raft__pb2.VoteReply.FromString,
                )
        self.AppendEntries = channel.unary_unary(
                '/raft.RaftService/AppendEntries',
                request_serializer=raft__pb2.AppendEntriesRequest.SerializeToString,
                response_deserializer=raft__pb2.AppendEntriesReply.FromString,
                )
        self.ServeClient = channel.unary_unary(
                '/raft.RaftService/ServeClient',
                request_serializer=raft__pb2.ServeClientRequest.SerializeToString,
                response_deserializer=raft__pb2.ServeClientReply.FromString,
                )


class RaftServiceServicer(object):
    """The Raft service definition.
    """

    def RequestVote(self, request, context):
        """Sends a vote request to other nodes
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AppendEntries(self, request, context):
        """Appends entries to the log
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ServeClient(self, request, context):
        """Client interaction with the leader for get/set requests
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_RaftServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RequestVote': grpc.unary_unary_rpc_method_handler(
                    servicer.RequestVote,
                    request_deserializer=raft__pb2.VoteRequest.FromString,
                    response_serializer=raft__pb2.VoteReply.SerializeToString,
            ),
            'AppendEntries': grpc.unary_unary_rpc_method_handler(
                    servicer.AppendEntries,
                    request_deserializer=raft__pb2.AppendEntriesRequest.FromString,
                    response_serializer=raft__pb2.AppendEntriesReply.SerializeToString,
            ),
            'ServeClient': grpc.unary_unary_rpc_method_handler(
                    servicer.ServeClient,
                    request_deserializer=raft__pb2.ServeClientRequest.FromString,
                    response_serializer=raft__pb2.ServeClientReply.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'raft.RaftService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class RaftService(object):
    """The Raft service definition.
    """

    @staticmethod
    def RequestVote(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/raft.RaftService/RequestVote',
            raft__pb2.VoteRequest.SerializeToString,
            raft__pb2.VoteReply.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AppendEntries(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/raft.RaftService/AppendEntries',
            raft__pb2.AppendEntriesRequest.SerializeToString,
            raft__pb2.AppendEntriesReply.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ServeClient(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/raft.RaftService/ServeClient',
            raft__pb2.ServeClientRequest.SerializeToString,
            raft__pb2.ServeClientReply.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)