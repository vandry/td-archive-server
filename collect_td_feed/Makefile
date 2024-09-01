all: td_feed_pb2.py td_feed_pb2_grpc.py

%_pb2.py: ../proto/%.proto
	protoc -I../proto --python_out=. $<

%_pb2_grpc.py: ../proto/%.proto
	python3 -m grpc_tools.protoc -I../proto --grpc_python_out=. $<
