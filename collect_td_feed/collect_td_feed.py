#!/usr/bin/python3

from concurrent import futures
import boto3
import datetime
import grpc
import io
import json
import logging
import os
import queue
import stomp
import struct
import sys
import threading
import time
import weakref

from google.protobuf import timestamp_pb2
import td_feed_pb2
import td_feed_pb2_grpc


def process_td_feed(body, feed):
    messages = []
    try:
        for outer_message in body:
            message = list(outer_message.values())[0]
            message_type = message["msg_type"]
            if message_type in ("CA", "CB", "CC"):
                timestamp_ms = int(message["time"])
                m = td_feed_pb2.TDFrame(
                    timestamp=timestamp_pb2.Timestamp(
                        seconds=timestamp_ms // 1000,
                        nanos=(timestamp_ms % 1000) * 1000000),
                    area_id=message["area_id"],
                    description=message["descr"])
                if message_type in ("CA", "CB"):
                    m.from_berth = message["from"]
                if message_type in ("CA", "CC"):
                    m.to_berth = message["to"]
                messages.append(m)
    except (ValueError, KeyError) as e:
        logging.error('Bad message %r: %s', body, e)
    for m in messages:
        feed.put(m)


class Listener(stomp.ConnectionListener):

    def __init__(self, feed):
        #self.acker = acker
        self.feed = feed

    def on_message(self, frame):
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)
        #self.acker(headers)
        if "TD_" in headers["destination"]:
            process_td_feed(parsed_body, self.feed)

    def on_message_for_older_stomp(self, headers, message_raw):
        parsed_body = json.loads(message_raw)
        #self.acker(headers)
        if "TD_" in headers["destination"]:
            process_td_feed(parsed_body, self.feed)

    def on_error(self, frame):
        logging.error('received an error %r', frame.body)

    def on_disconnected(self):
        logging.error('disconnected')


class Subscriber(object):

    def __init__(self, query):
        self._cancel_event = threading.Event()
        self._queue = queue.Queue(maxsize=2000)
        self._query = query

    def __iter__(self):
        return self

    def __next__(self):
        m = self._queue.get()
        if self._cancel_event.is_set():
            raise StopIteration
        return m

    def submit(self, m: td_feed_pb2.TDFrame):
        if self._query.area_id:
            if m.area_id not in self._query.area_id:
                return
        if self._query.description:
            if m.description not in self._query.description:
                return
        self._queue.put_nowait(m)

    def cancel(self):
        self._cancel_event.set()
        try:
            self._queue.put_nowait(td_feed_pb2.TDFrame())
        except queue.Full:
            pass


class Saver(object):

    def __init__(self, storage_endpoint):
        self.accumulators = {}
        self.s3_client = boto3.client('s3', endpoint_url=storage_endpoint)

    def submit(self, m: td_feed_pb2.TDFrame):
        midnight = m.timestamp.seconds - (m.timestamp.seconds % 86400)
        try:
            f = self.accumulators[midnight]
        except KeyError:
            f = []
            self.accumulators[midnight] = f

        ser = m.SerializeToString()
        packet = struct.pack('<L', len(ser)) + ser
        f.append(packet)

    def flush(self):
        accumulators, self.accumulators = self.accumulators, {}
        for midnight, f in accumulators.items():
            datestamp = datetime.datetime.utcfromtimestamp(midnight).strftime('%Y%m%d')
            filename = 'batch.%s.%d.%d.%s' % (datestamp, time.time(), os.getpid(), os.environ.get('HOSTNAME', ''))
            contents = io.BytesIO(b''.join(f))
            try:
                self.s3_client.upload_fileobj(contents, 'spool', filename)
            except Exception as e:
                logging.error('Uploading spool file: %s; dropping %d frames', e, len(f))


class TDFeed(td_feed_pb2_grpc.TDFeedServicer):

    def __init__(self, credentials_filename):
        with open(credentials_filename) as f:
            feed_username, feed_password = (l.strip() for l in f)
        self._connect_headers = {
            'username': feed_username,
            'passcode': feed_password,
            'wait': True,
            #'client-id': '1fb38914-2506-45dc-8b2d-ce3a30bed2d3',
        }
        self.subscribers = weakref.WeakKeyDictionary()
        self.count = 0

    def start(self):
        connection = stomp.Connection(
            [('publicdatafeeds.networkrail.co.uk', 61618)],
            reconnect_attempts_max=999999999,
            keepalive=True, heartbeats=(5000, 5000))
        #def ack(headers):
        #    connection.ack(id=headers['ack'], subscription=headers['subscription'])
        connection.set_listener('', Listener(self))
        #connection.start()
        connection.connect(**self._connect_headers)
        subscribe_headers = {
            'destination': '/topic/TD_ALL_SIG_AREA',
            'id': 1,
            #'activemq.subscriptionName': '1fb38914-2506-45dc-8b2d-ce3a30bed2d3-td',
            #'ack': 'client-individual',
            'ack': 'auto',
        }
        connection.subscribe(**subscribe_headers)
        return connection

    def put(self, m):
        self.count += 1
        subscribers = list(self.subscribers.keys())
        for s in subscribers:
            try:
                s.submit(m)
            except queue.Full:
                s.cancel()
                del self.subscribers[s]
                logging.warn('booted a subscriber for growing its queue too much')

    def Feed(self, query: td_feed_pb2.TDQuery, context):
        s = Subscriber(query)
        context.add_callback(s.cancel)
        self.subscribers[s] = None
        return s


def main():
    _, credentials_filename, storage_endpoint = sys.argv
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s: %(levelname)s: %(message)s')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    feed = TDFeed(credentials_filename)
    td_feed_pb2_grpc.add_TDFeedServicer_to_server(feed, server)
    port = '[::]:3000'
    server.add_insecure_port(port)
    server.start()
    logging.info('listening on ' + port)
    interval_start = time.time()
    saver = Saver(storage_endpoint)
    feed.subscribers[saver] = None
    while True:
        feed_conn = feed.start()
        while feed_conn.is_connected():
            time.sleep(10)
            now = time.time()
            elapsed = now - interval_start
            if elapsed >= 60:
                count = feed.count
                feed.count = 0
                logging.warn('Have %d subscribers and sent %f messages/second', len(feed.subscribers), count/elapsed)
                interval_start = now
                saver.flush()
        logging.error('Need to reconnect')


if __name__ == '__main__':
    main()
