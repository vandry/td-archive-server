FROM python:3

WORKDIR /root

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install grpcio protobuf==3.20.3 stomp-py boto3

EXPOSE 3000

COPY collect_td_feed.py td_feed_pb2_grpc.py td_feed_pb2.py /root/
CMD ["python3", "collect_td_feed.py"]
