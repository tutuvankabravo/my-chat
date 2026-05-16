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

# === HTML ВСТРОЕН В КОД ===
HTML_PAGE = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
    <title>💬 Веб-чат | Общайтесь в реальном времени</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root {
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --text-primary: #f0f6fc;
            --text-secondary: #8b949e;
            --accent: #58a6ff;
            --accent-hover: #1f6feb;
            --border: #30363d;
            --success: #238636;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            height: 100vh;
            overflow: hidden;
        }
        .chat-container {
            display: flex;
            flex-direction: column;
            height: 100vh;
            max-width: 1400px;
            margin: 0 auto;
        }
        .chat-header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
        .chat-title h1 { color: var(--accent); font-size: 1.3em; }
        .online-status {
            background: var(--success);
            color: white;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.8em;
        }
        .username-display {
            background: var(--bg-tertiary);
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.9em;
        }
        .change-name-btn {
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 5px 12px;
            border-radius: 20px;
            cursor: pointer;
        }
        .change-name-btn:hover { background: var(--accent); }
        .chat-main { display: flex; flex: 1; overflow: hidden; }
        .users-sidebar {
            width: 250px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--border);
            display: flex;
            flex-direction: column;
        }
        .users-header { padding: 15px; border-bottom: 1px solid var(--border); font-weight: bold; background: var(--bg-tertiary); }
        .users-list { flex: 1; overflow-y: auto; padding: 10px; }
        .user-item { padding: 8px 12px; margin: 4px 0; border-radius: 8px; display: flex; align-items: center; gap: 8px; }
        .user-item:hover { background: var(--bg-tertiary); }
        .user-avatar { width: 8px; height: 8px; border-radius: 50%; background: var(--success); }
        .messages-area { flex: 1; display: flex; flex-direction: column; }
        .messages-container { flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }
        .message { display: flex; animation: fadeIn 0.3s ease; }
        .message.system { justify-content: center; }
        .message.system .message-bubble { background: var(--bg-tertiary); color: var(--text-secondary); font-size: 0.85em; padding: 6px 15px; border-radius: 20px; }
        .message.own { justify-content: flex-end; }
        .message-bubble { max-width: 70%; padding: 10px 15px; border-radius: 18px; }
        .message:not(.own) .message-bubble { background: var(--bg-tertiary); border-bottom-left-radius: 4px; }
        .message.own .message-bubble { background: var(--accent); border-bottom-right-radius: 4px; }
        .message-username { font-size: 0.75em; font-weight: bold; margin-bottom: 4px; color: var(--accent); }
        .message-text { font-size: 0.95em; word-wrap: break-word; }
        .message-time { font-size: 0.7em; opacity: 0.7; margin-top: 4px; text-align: right; }
        .typing-indicator { padding: 8px 20px; font-size: 0.85em; color: var(--text-secondary); font-style: italic; min-height: 36px; }
        .input-area { background: var(--bg-secondary); border-top: 1px solid var(--border); padding: 15px 20px; display: flex; gap: 10px; }
        .message-input { flex: 1; background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 12px 15px; border-radius: 25px; resize: none; font-family: inherit; outline: none; }
        .message-input:focus { border-color: var(--accent); }
        .send-btn { background: var(--accent); color: white; border: none; padding: 0 25px; border-radius: 25px; cursor: pointer; font-weight: bold; }
        .send-btn:hover { background: var(--accent-hover); transform: scale(1.02); }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        @media (max-width: 768px) { .users-sidebar { display: none; } .message-bubble { max-width: 85%; } }
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: var(--bg-primary); }
        ::-webkit-scrollbar-thumb { background: var(--bg-tertiary); border-radius: 4px; }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-header">
            <div class="chat-title"><h1>💬 Веб-чат</h1><span class="online-status" id="onlineCount">0 онлайн</span></div>
            <div class="user-info"><span class="username-display" id="currentUsername">Загрузка...</span><button class="change-name-btn" onclick="changeUsername()">Сменить имя</button></div>
        </div>
        <div class="chat-main">
            <div class="users-sidebar"><div class="users-header">👥 Участники (<span id="usersCount">0</span>)</div><div class="users-list" id="usersList"><div style="text-align: center; color: var(--text-secondary);">Подключение...</div></div></div>
            <div class="messages-area"><div class="messages-container" id="messagesContainer"></div><div class="typing-indicator" id="typingIndicator"></div><div class="input-area"><textarea id="messageInput" class="message-input" placeholder="Введите сообщение..." rows="1" onkeypress="handleKeyPress(event)"></textarea><button class="send-btn" onclick="sendMessage()">📨 Отправить</button></div></div>
        </div>
    </div>
    <script>
        let ws = null, currentUser = null, typingTimeout = null, isTyping = false, typingUsers = new Set();
        const messagesContainer = document.getElementById('messagesContainer'), messageInput = document.getElementById('messageInput'), typingIndicator = document.getElementById('typingIndicator'), currentUsernameSpan = document.getElementById('currentUsername'), onlineCountSpan = document.getElementById('onlineCount'), usersCountSpan = document.getElementById('usersCount'), usersList = document.getElementById('usersList');
        
        function connect(username) {
            const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
            ws = new WebSocket(wsUrl);
            ws.onopen = () => { console.log('Connected'); ws.send(JSON.stringify({ username: username })); setInterval(() => { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' })); }, 30000); };
            ws.onmessage = (event) => { const data = JSON.parse(event.data); handleMessage(data); };
            ws.onerror = (error) => console.error(error);
            ws.onclose = () => { console.log('Disconnected'); showSystemMessage('Соединение потеряно. Переподключение...'); setTimeout(() => { if (currentUser) connect(currentUser); }, 3000); };
        }
        
        function handleMessage(data) {
            switch(data.type) {
                case 'message': addMessageToChat(data); break;
                case 'system': showSystemMessage(data.message); if (data.users_count) updateOnlineCount(data.users_count); break;
                case 'users_list': updateUsersList(data.users, data.count); break;
                case 'typing': updateTypingIndicator(data.username, data.is_typing); break;
            }
        }
        
        function addMessageToChat(message) {
            const messageDiv = document.createElement('div');
            messageDiv.className = `message ${message.username === currentUser ? 'own' : ''}`;
            messageDiv.innerHTML = `<div class="message-bubble"><div class="message-username">${escapeHtml(message.username)}</div><div class="message-text">${escapeHtml(message.text)}</div><div class="message-time">${formatTime(message.timestamp)}</div></div>`;
            messagesContainer.appendChild(messageDiv);
            scrollToBottom();
        }
        
        function showSystemMessage(text) { const messageDiv = document.createElement('div'); messageDiv.className = 'message system'; messageDiv.innerHTML = `<div class="message-bubble">${escapeHtml(text)}</div>`; messagesContainer.appendChild(messageDiv); scrollToBottom(); }
        function sendMessage() { const text = messageInput.value.trim(); if (!text || !ws) return; ws.send(JSON.stringify({ type: 'message', text: text })); messageInput.value = ''; if (isTyping) { ws.send(JSON.stringify({ type: 'typing', is_typing: false })); isTyping = false; } }
        function handleKeyPress(event) { if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); sendMessage(); } if (!isTyping && messageInput.value.length > 0 && ws) { isTyping = true; ws.send(JSON.stringify({ type: 'typing', is_typing: true })); } clearTimeout(typingTimeout); typingTimeout = setTimeout(() => { if (isTyping && ws) { isTyping = false; ws.send(JSON.stringify({ type: 'typing', is_typing: false })); } }, 1000); }
        function updateUsersList(users, count) { usersCountSpan.textContent = count; onlineCountSpan.textContent = `${count} онлайн`; if (users.length === 0) { usersList.innerHTML = '<div style="text-align: center; color: var(--text-secondary);">Нет пользователей</div>'; return; } usersList.innerHTML = users.map(user => `<div class="user-item"><div class="user-avatar"></div><div class="user-name">${escapeHtml(user)} ${user === currentUser ? '(Вы)' : ''}</div></div>`).join(''); }
        function updateOnlineCount(count) { onlineCountSpan.textContent = `${count} онлайн`; usersCountSpan.textContent = count; }
        function updateTypingIndicator(username, isTypingUser) { if (isTypingUser && username !== currentUser) typingUsers.add(username); else typingUsers.delete(username); if (typingUsers.size > 0) { const names = Array.from(typingUsers); let text = names.length === 1 ? `${names[0]} печатает...` : names.length === 2 ? `${names[0]} и ${names[1]} печатают...` : `${names.length} человек печатают...`; typingIndicator.textContent = text; } else typingIndicator.textContent = ''; }
        function changeUsername() { const newName = prompt('Введите новое имя (макс. 20 символов):', currentUser); if (newName && newName.trim() && newName.trim() !== currentUser) { currentUser = newName.trim().substring(0, 20); currentUsernameSpan.textContent = currentUser; localStorage.setItem('chat_username', currentUser); if (ws) ws.close(); setTimeout(() => connect(currentUser), 100); } }
        function escapeHtml(text) { const div = document.createElement('div'); div.textContent = text; return div.innerHTML; }
        function formatTime(timestamp) { if (!timestamp) return ''; const date = new Date(timestamp); return date.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }); }
        function scrollToBottom() { messagesContainer.scrollTop = messagesContainer.scrollHeight; }
        
        const savedUsername = localStorage.getItem('chat_username');
        if (savedUsername) currentUser = savedUsername;
        else { currentUser = prompt('Введите ваше имя для чата:', 'Гость') || `Гость_${Math.floor(Math.random() * 1000)}`; localStorage.setItem('chat_username', currentUser); }
        currentUsernameSpan.textContent = currentUser;
        connect(currentUser);
    </script>
</body>
</html>'''

# === HTTP ОБРАБОТЧИК ===
async def handle_index(request):
    """Отдает HTML страницу чата"""
    return web.Response(text=HTML_PAGE, content_type='text/html; charset=utf-8')

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

async def health_check(request):
    return web.Response(text="OK")

# === ЗАПУСК СЕРВЕРА ===
app = web.Application()
app.router.add_get('/', handle_index)
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/healthz', health_check)

if __name__ == "__main__":
    print(f"🚀 Чат-сервер запущен на порту {PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)