# -*- coding: utf-8 -*-

"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT License.
"""
import grpc

from dapr.proto import appcallback_service_v1, common_v1, appcallback_v1
from google.protobuf.any_pb2 import Any
from typing import Callable, Optional, List

SUBSCRIBER_CALLBACK = Callable[[appcallback_v1.TopicEventRequest], None]

class SubscriberCallback:
    def __init__(
            self,
            topic: str,
            callback: SUBSCRIBER_CALLBACK,
            metadata: Optional[Dict[str, str]]):
        self._topic = topic
        self._callback = callback
        self._metadata = metadata

    @property
    def callback(self) -> SUBSCRIBER_CALLBACK:
        return self._callback

    def as_topic_subscription(self) -> appcallback_v1.TopicSubscription:
        return appcallback_v1.TopicSubscription(topic=self._topic, metadata=self._metadata)


class AppCallbackServicer(appcallback_service_v1.AppCallbackServicer):
    def __init__(self):
        self._invoke_method_map = {}
        self._subscription_map = {}
        self._binding_map = {}

    def register_method(self, method: str, cb: Callable[[Any], Any]) -> None:
        if method in self._invoke_method_map:
            raise ValueError(f'{method} is already registered')
        self._invoke_method_map[method] = cb
    
    def register_subscriber(
            self,
            topic: str,
            cb: SUBSCRIBER_CALLBACK,
            metadata: Optional[Dict[str, str]]) -> None:
        if topic in self._subscription_map:
            raise ValueError(f'{topic} is already registered')

        self._subscription_map[topic] = SubscriberCallback(topic, cb, metadata)

    def register_input_binding(
            self,
            name: str,
            cb: Callable[[bytes, Optional[Dict[str, str]]], Any]) -> None:
        if name in self._binding_map:
            raise ValueError(f'{name} is already registered')
        self._binding_map[name] = cb

    def OnInvoke(self, request: common_v1.InvokeRequest, context) -> common_v1.InvokeResponse:
        """Invokes service method with InvokeRequest.
        """
        if request.method not in self._invoke_method_map:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            raise NotImplementedError(f'{request.method} method not implemented!')
        return self._invoke_method_map[request.method](request)

    def ListTopicSubscriptions(self, request, context) -> appcallback_v1.ListTopicSubscriptionsResponse:
        """Lists all topics subscribed by this app.
        """
        topics = []
        for name in self._subscription_map:
            topics.append(self._subscription_map[name].as_topic_subscription())
        return appcallback_v1.ListTopicSubscriptionsResponse(subscriptions=topics)

    def OnTopicEvent(self, request: appcallback_v1.TopicEventRequest, context) -> None:
        """Subscribes events from Pubsub
        """
        if request.topic not in self._subscription_map:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            raise NotImplementedError(f'topic {request.topic} is not implemented!')
        self._subscription_map[request.topic].callback(request)

    def ListInputBindings(self, request, context) -> appcallback_v1.ListInputBindingsResponse:
        """Lists all input bindings subscribed by this app.
        """
        bindings = []
        for name in self._binding_map:
            bindings.append(name)
        return appcallback_v1.ListInputBindingsResponse(bindings=bindings)

    def OnBindingEvent(self, request: appcallback_v1.BindingEventRequest, context) -> appcallback_v1.BindingEventResponse:
        """Listens events from the input bindings

        User application can save the states or send the events to the output
        bindings optionally by returning BindingEventResponse.
        """
        if request.name not in self._binding_map:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            raise NotImplementedError(f'{request.name} binding not implemented!')
        return self._binding_map[request.name](request.data, request.metadata)
