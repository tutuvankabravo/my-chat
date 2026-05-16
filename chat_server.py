import asyncio
import json
import websockets
import http
import signal
import os
from datetime import datetime
import hashlib

# === НАСТРОЙКИ ===
PORT = int(os.environ.get("PORT", 8080))
# =================

messages_history = []
MAX_HISTORY = 100
connected_clients = set()

class ChatServer:
    def __init__(self):
        self.clients = {}
    
    async def register(self, websocket, username):
        self.clients[websocket] = username
        connected_clients.add(websocket)
        
        if messages_history:
            for msg in messages_history[-50:]:
                try:
                    await websocket.send(json.dumps(msg))
                except:
                    pass
        
        await self.broadcast({
            'type': 'system',
            'message': f'👋 {username} присоединился к чату',
            'timestamp': datetime.now().isoformat(),
            'users_count': len(self.clients)
        })
        await self.broadcast_users_list()
    
    async def unregister(self, websocket):
        if websocket in self.clients:
            username = self.clients[websocket]
            del self.clients[websocket]
            connected_clients.discard(websocket)
            await self.broadcast({
                'type': 'system',
                'message': f'👋 {username} покинул чат',
                'timestamp': datetime.now().isoformat(),
                'users_count': len(self.clients)
            })
            await self.broadcast_users_list()
    
    async def broadcast(self, message):
        if not connected_clients:
            return
        tasks = []
        for client in connected_clients:
            try:
                tasks.append(client.send(json.dumps(message)))
            except:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def broadcast_users_list(self):
        users_list = list(self.clients.values())
        await self.broadcast({
            'type': 'users_list',
            'users': users_list,
            'count': len(users_list)
        })
    
    async def handle_message(self, websocket, message_data):
        username = self.clients.get(websocket)
        if not username:
            return
        
        message_type = message_data.get('type', 'message')
        
        if message_type == 'message':
            message = {
                'type': 'message',
                'username': username,
                'text': message_data.get('text', ''),
                'timestamp': datetime.now().isoformat(),
                'id': hashlib.md5(f"{username}{datetime.now()}".encode()).hexdigest()[:8]
            }
            messages_history.append(message)
            if len(messages_history) > MAX_HISTORY:
                messages_history.pop(0)
            await self.broadcast(message)
        
        elif message_type == 'typing':
            await self.broadcast({
                'type': 'typing',
                'username': username,
                'is_typing': message_data.get('is_typing', False)
            })
        
        elif message_type == 'ping':
            await websocket.send(json.dumps({'type': 'pong'}))

chat_instance = ChatServer()

def health_check(connection, request):
    if request.path == "/healthz":
        return connection.respond(http.HTTPStatus.OK, b"OK\n")

async def websocket_handler(websocket):
    chat_server = chat_instance
    
    try:
        await websocket.send(json.dumps({
            'type': 'greeting',
            'message': 'Добро пожаловать в чат! Представьтесь:'
        }))
        
        response = await websocket.recv()
        data = json.loads(response)
        username = data.get('username', '').strip()
        
        if not username:
            username = f"Гость_{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6]}"
        
        username = username[:20]
        await chat_server.register(websocket, username)
        
        async for message in websocket:
            try:
                data = json.loads(message)
                await chat_server.handle_message(websocket, data)
            except json.JSONDecodeError:
                pass
    
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await chat_server.unregister(websocket)

async def main():
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    
    try:
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    except NotImplementedError:
        pass
    
    async with websockets.serve(
        websocket_handler, 
        "0.0.0.0", 
        PORT, 
        process_request=health_check
    ):
        print(f"🚀 WebSocket сервер запущен на порту {PORT}")
        await stop

if __name__ == "__main__":
    asyncio.run(main())