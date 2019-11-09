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
    allow_anonymous = False

    async def reader(self, channel: aioredis.pubsub.Channel, websocket: WebSocket) -> None:
        while await channel.wait_message():
            message = await channel.get_json()
            log.debug(f'redis channel reader: {message}')
            await websocket.send_json(message)

    def get_channel_name(self, websocket: WebSocket) -> str:
        return websocket.path_params.get('channel', 'channel-1')

    async def on_connect(self, websocket: WebSocket) -> None:
        if not websocket.user.is_authenticated and not self.allow_anonymous:
            await websocket.close(status.WS_1008_POLICY_VIOLATION)
            log.warning(f"unauthenticated connection attempt")
            return
        await super().on_connect(websocket)
        self.username = websocket.user.username if websocket.user.is_authenticated else 'anonymous'
        self.chatname = None
        self.channel_name = self.get_channel_name(websocket)
        self.redis_channel_name = self.redis_channel_separator.join([self.redis_channel_prefix, self.channel_name])
        self.redis_pub = await aioredis.create_redis(self.redis_url)
        self.redis_sub = await aioredis.create_redis(self.redis_url)
        self.redis_sub_channel = (await self.redis_sub.subscribe(self.redis_channel_name))[0]
        asyncio.get_running_loop().create_task(self.reader(self.redis_sub_channel, websocket))
        log.info(f"chat connect on channel {self.channel_name} by {self.username}")

    async def on_receive(self, websocket: WebSocket, data: typing.Any) -> None:
        action = data.get('action')
        content = data.get('content')
        response = {'channel': self.channel_name,
                    'username': self.username,
                    'action': data.get('action'),
                    }
        if action == 'new_user':
            self.chatname = content['chatname']
            response = { 'action': action,
                         'chatname': self.chatname,
                         'content': content,
                        }
            await self.redis_pub.publish_json(self.redis_channel_name, response)
        elif action == 'send_message':
            if self.chatname is None:
                log.error(f"action=send_message on channel {self.channel_name} by {self.username}"
                          " did not establish a chatname")
                return
            response = { 'action': action,
                         'chatname': self.chatname,
                         'content': content,
                        }
            await self.redis_pub.publish_json(self.redis_channel_name, response)

    async def on_disconnect(self, websocket: WebSocket, close_code: int):
        await super().on_disconnect(websocket, close_code)
        try:
            self.redis_pub.close()
            self.redis_sub.close()
        except AttributeError:
            pass
