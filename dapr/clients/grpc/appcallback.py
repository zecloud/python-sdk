# -*- coding: utf-8 -*-

"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT License.
"""
import grpc
import functools

from concurrent import futures
from dapr.clients.grpc._appcallback_servicier import AppCallbackServicer
from dapr.proto import appcallback_service_v1, common_v1

class AppCallback:
    def __init__(self):
        self._servicer = AppCallbackServicer()
    
    def __del__(self):
        self.stop()
    
    def start(self):
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        appcallback_service_v1.add_AppCallbackServicer_to_server(self._servicer, self._server)
        self._server.add_insecure_port('[::]:50051')
        self._server.start()

    def stop(self):
        self._server.stop(0)

    def invoke_method(self, name: str):
        @functools.wraps(func)
        def wrapper(func):
            self._servicer.register_method(name, func)
            return func
        return wrapper
    
    def subscriber(self, topic: str, metadata: Optional[Dict[str, str]]):
        @functools.wraps(func)
        def wrapper(func):
            self._servicer.register_subscriber(topic, func, metadata)
            return func
        return wrapper

    def input_binding(self, name: str):
        @functools.wraps(func)
        def wrapper(func):
            self._servicer.register_input_binding(name, func)
            return func
        return wrapper



app = AppCallback()
app.start()

@app.invoke_method(method="Method1")
def method1(req):
    # implement method1
    return ""

@app.subscriber(topic="topic1")
def topic1(req):
    
