import typing
import logging
import asyncio
import aioredis
from starlette import status
from starlette.endpoints import WebSocketEndpoint
from starlette.websockets import WebSocket

log = logging.getLogger(__name__)


class ChatWebSocketEndpoint(WebSocketEndpoint):

    encoding = 'json'
    redis_url = 'redis://localhost'
    redis_channel_prefix = 'chatlette'
    redis_channel_separator = '/'
    accept_receive = False

    async def reader(self, channel: aioredis.pubsub.Channel, websocket: WebSocket) -> None:
        while await channel.wait_message():
            message = await channel.get_json()
            log.debug(f'redis channel reader: {message}')
            await websocket.send_json(message)

    def get_channel_name(self, websocket: WebSocket) -> str:
        return websocket.path_params.get('channel', 'channel-1')

    async def on_connect(self, websocket: WebSocket) -> None:
        await super().on_connect(websocket)
        if not websocket.user.is_authenticated:
            await websocket.close(status.WS_1008_POLICY_VIOLATION)
            log.warning(f"closing unauthenticated connection")
            return
        self.accept_receive = True
        self.user = websocket.user
        self.channel_name = self.get_channel_name(websocket)
        self.redis_channel_name = self.redis_channel_separator.join([self.redis_channel_prefix, self.channel_name])
        self.redis_pub = await aioredis.create_redis(self.redis_url)
        self.redis_sub = await aioredis.create_redis(self.redis_url)
        self.redis_sub_channel = (await self.redis_sub.subscribe(self.redis_channel_name))[0]
        asyncio.get_running_loop().create_task(self.reader(self.redis_sub_channel, websocket))
        log.info(f"chat connect on channel {self.channel_name} by {self.user.username}")

    async def on_receive(self, websocket: WebSocket, data: typing.Any) -> None:
        if not self.accept_receive:
            log.warning(f"on_receive: {data} but not on_receive not allowed")
            return
        log.info(f"on_receive: {data}")
        action = data.get('action')
        content = data.get('content')
        try:            
            action_func = getattr(self, f'action_{action}')
        except AttributeError:
            raise NotImplementedError(f"action_{action} not implemented")
        await action_func(content)
            
    async def publish_all(self, response: typing.Any) -> None:
        await self.redis_pub.publish_json(self.redis_channel_name, response)

    async def on_disconnect(self, websocket: WebSocket, close_code: int):
        await super().on_disconnect(websocket, close_code)
        try:
            self.redis_pub.close()
            self.redis_sub.close()
        except AttributeError:
            pass
