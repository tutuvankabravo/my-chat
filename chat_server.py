import asyncio
import json
import websockets
from datetime import datetime
import hashlib
import os
from aiohttp import web

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

# === HTTP ОБРАБОТЧИК ДЛЯ ОТДАЧИ HTML ===
async def handle_index(request):
    """Отдает файл chat.html"""
    try:
        with open('chat.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html; charset=utf-8')
    except:
        return web.Response(text="<h1>File chat.html not found</h1>", content_type='text/html', status=404)

# === WEBSOCKET ОБРАБОТЧИК ===
async def websocket_handler(request):
    """Обработчик WebSocket соединений"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    try:
        # Ждем имя пользователя
        greeting = await ws.receive()
        if greeting.type != web.WSMsgType.TEXT:
            return ws
        
        data = json.loads(greeting.data)
        username = data.get('username', '').strip()
        
        if not username:
            username = f"Гость_{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6]}"
        
        username = username[:20]
        
        # Регистрируем пользователя
        await chat_instance.register(ws, username)
        
        # Основной цикл обработки сообщений
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await chat_instance.handle_message(ws, data)
                except json.JSONDecodeError:
                    pass
            elif msg.type == web.WSMsgType.ERROR:
                break
    
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        await chat_instance.unregister(ws)
    
    return ws

# === ЗАПУСК СЕРВЕРА ===
app = web.Application()
app.router.add_get('/', handle_index)
app.router.add_get('/chat.html', handle_index)
app.router.add_get('/ws', websocket_handler)

# Добавляем health check для Render
async def health_check(request):
    return web.Response(text="OK")
app.router.add_get('/healthz', health_check)

# Запускаем приложение
if __name__ == "__main__":
    web.run_app(app, host='0.0.0.0', port=PORT)