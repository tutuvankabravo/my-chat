import asyncio
import json
import os
from datetime import datetime
import hashlib
from aiohttp import web

# === НАСТРОЙКИ ===
PORT = int(os.environ.get("PORT", 8080))
# =================

# --- Хранилище данных чата ---
messages_history = []
MAX_HISTORY = 100
connected_clients = set()

class ChatServer:
    def __init__(self):
        self.clients = {}  # {websocket: {'username': username, 'session_id': session_id}}
        self.user_sessions = {}  # {'username_session': count}

    async def register(self, ws, username, session_id):
        # Сохраняем клиента с его сессией
        self.clients[ws] = {'username': username, 'session_id': session_id}
        connected_clients.add(ws)
        
        # Увеличиваем счетчик сессий для этого пользователя
        session_key = f"{username}_{session_id}"
        self.user_sessions[session_key] = self.user_sessions.get(session_key, 0) + 1

        # Отправляем историю новому пользователю
        for msg in messages_history[-50:]:
            try:
                await ws.send_str(json.dumps(msg))
            except:
                pass

        # Если это первая сессия пользователя, показываем вход
        if self.user_sessions[session_key] == 1:
            await self.broadcast({
                'type': 'system',
                'message': f'👋 {username} присоединился к чату',
                'users_count': len(self.get_unique_users())
            })
        
        await self.broadcast_users_list()

    async def unregister(self, ws):
        if ws in self.clients:
            client_data = self.clients[ws]
            username = client_data['username']
            session_id = client_data['session_id']
            session_key = f"{username}_{session_id}"
            
            del self.clients[ws]
            connected_clients.discard(ws)
            
            # Уменьшаем счетчик сессий
            self.user_sessions[session_key] = self.user_sessions.get(session_key, 1) - 1
            if self.user_sessions[session_key] <= 0:
                del self.user_sessions[session_key]
                # Если это была последняя сессия пользователя, показываем выход
                await self.broadcast({
                    'type': 'system',
                    'message': f'👋 {username} покинул чат',
                    'users_count': len(self.get_unique_users())
                })
            
            await self.broadcast_users_list()

    def get_unique_users(self):
        """Возвращает уникальных пользователей с количеством их сессий"""
        user_sessions_count = {}
        for client_data in self.clients.values():
            username = client_data['username']
            user_sessions_count[username] = user_sessions_count.get(username, 0) + 1
        
        return [{'name': name, 'sessions': count} for name, count in user_sessions_count.items()]

    async def broadcast(self, message):
        if not connected_clients:
            return
        message_json = json.dumps(message)
        for client in list(connected_clients):
            try:
                if not client.closed:
                    await client.send_str(message_json)
            except:
                pass

    async def broadcast_users_list(self):
        users_list = self.get_unique_users()
        await self.broadcast({
            'type': 'users_list',
            'users': users_list,
            'count': len(users_list)
        })

    async def handle_message(self, ws, data):
        if ws not in self.clients:
            return
        
        client_data = self.clients[ws]
        username = client_data['username']

        msg_type = data.get('type', 'message')

        if msg_type == 'message':
            message = {
                'type': 'message',
                'username': username,
                'text': data.get('text', ''),
                'timestamp': datetime.now().isoformat(),
                'id': hashlib.md5(f"{username}{datetime.now()}".encode()).hexdigest()[:8]
            }
            messages_history.append(message)
            if len(messages_history) > MAX_HISTORY:
                messages_history.pop(0)
            await self.broadcast(message)

        elif msg_type == 'typing':
            await self.broadcast({
                'type': 'typing',
                'username': username,
                'is_typing': data.get('is_typing', False)
            })

        elif msg_type == 'ping':
            await ws.send_str(json.dumps({'type': 'pong'}))

chat_processor = ChatServer()

# --- Встроенный HTML (полностью адаптированный) ---
HTML_PAGE = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no, viewport-fit=cover">
    <meta name="theme-color" content="#0d1117">
    <title>💬 Веб-чат</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root {
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --text-primary: #f0f6fc;
            --text-secondary: #8b949e;
            --accent: #58a6ff;
            --border: #30363d;
            --success: #238636;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            height: 100vh;
            overflow: hidden;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
        }
        .chat-container {
            display: flex;
            flex-direction: column;
            height: 100vh;
            height: -webkit-fill-available;
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .input-area {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 10px 12px;
            padding-top: max(10px, env(safe-area-inset-top));
            display: flex;
            gap: 8px;
            flex-shrink: 0;
            order: 0;
        }
        
        .chat-header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 8px 12px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 6px;
            flex-shrink: 0;
            order: 1;
        }
        
        .chat-main {
            display: flex;
            flex: 1;
            overflow: hidden;
            min-height: 0;
            order: 2;
            flex-direction: column;
        }
        
        .toggle-users-btn {
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 5px 10px;
            border-radius: 20px;
            cursor: pointer;
            font-size: 0.8em;
        }
        
        .users-sidebar {
            width: 220px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--border);
            display: none;
            flex-direction: column;
            overflow: hidden;
        }
        
        .users-sidebar.show {
            display: flex;
        }
        
        .users-header { padding: 10px; border-bottom: 1px solid var(--border); font-weight: bold; background: var(--bg-tertiary); font-size: 0.85em; }
        .users-list { flex: 1; overflow-y: auto; padding: 8px; }
        .user-item { padding: 6px 10px; margin: 2px 0; border-radius: 8px; display: flex; align-items: center; gap: 8px; font-size: 0.85em; }
        .user-item:active { background: var(--bg-tertiary); }
        .user-avatar { width: 8px; height: 8px; border-radius: 50%; background: var(--success); flex-shrink: 0; }
        .user-name { word-break: break-word; flex: 1; }
        .user-sessions { font-size: 0.7em; color: var(--text-secondary); margin-left: 4px; }
        
        .messages-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        
        .messages-container {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        
        .message { display: flex; animation: fadeIn 0.3s ease; }
        .message.system { justify-content: center; }
        .message.system .message-bubble { background: var(--bg-tertiary); color: var(--text-secondary); font-size: 0.75em; padding: 5px 12px; border-radius: 20px; }
        .message.own { justify-content: flex-end; }
        .message-bubble { max-width: 80%; padding: 8px 12px; border-radius: 18px; }
        .message:not(.own) .message-bubble { background: var(--bg-tertiary); border-bottom-left-radius: 4px; }
        .message.own .message-bubble { background: var(--accent); border-bottom-right-radius: 4px; }
        .message-username { font-size: 0.7em; font-weight: bold; margin-bottom: 3px; color: var(--accent); }
        .message-text { font-size: 0.85em; word-wrap: break-word; white-space: pre-wrap; }
        .message-time { font-size: 0.6em; opacity: 0.7; margin-top: 3px; text-align: right; }
        .typing-indicator { padding: 6px 16px; font-size: 0.75em; color: var(--text-secondary); font-style: italic; min-height: 32px; background: var(--bg-primary); flex-shrink: 0; }
        
        .message-input {
            flex: 1;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 0.9em;
            font-family: inherit;
            outline: none;
            resize: none;
            overflow-y: auto;
            overflow-x: hidden;
            line-height: 1.4;
            max-height: 120px;
            min-height: 40px;
            scrollbar-width: thin;
        }
        
        .message-input::-webkit-scrollbar {
            width: 4px;
        }
        
        .message-input::-webkit-scrollbar-track {
            background: var(--bg-secondary);
            border-radius: 4px;
        }
        
        .message-input::-webkit-scrollbar-thumb {
            background: var(--accent);
            border-radius: 4px;
        }
        
        .message-input:focus { border-color: var(--accent); }
        
        .send-btn {
            background: var(--accent);
            color: white;
            border: none;
            padding: 0 20px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: bold;
            font-size: 0.85em;
            white-space: nowrap;
        }
        .send-btn:active { background: #1f6feb; transform: scale(0.98); }
        
        .chat-title h1 { color: var(--accent); font-size: 1.1em; }
        .online-status { background: var(--success); color: white; padding: 3px 8px; border-radius: 20px; font-size: 0.7em; }
        .username-display { background: var(--bg-tertiary); padding: 3px 8px; border-radius: 20px; font-size: 0.8em; }
        .change-name-btn { background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 3px 8px; border-radius: 20px; cursor: pointer; font-size: 0.75em; }
        
        @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
        
        @media (max-width: 768px) {
            .message-bubble { max-width: 85%; }
            .input-area { padding: 8px 12px; padding-top: max(8px, env(safe-area-inset-top)); }
            .message-input { padding: 8px 12px; font-size: 0.85em; border-radius: 6px; }
            .send-btn { padding: 0 16px; border-radius: 6px; }
            .chat-header { padding: 6px 10px; }
        }
        
        @supports (padding-top: env(safe-area-inset-top)) {
            .input-area {
                padding-top: max(10px, env(safe-area-inset-top));
            }
        }
        
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: var(--bg-primary); }
        ::-webkit-scrollbar-thumb { background: var(--bg-tertiary); border-radius: 3px; }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="input-area">
            <textarea id="messageInput" class="message-input" placeholder="Введите сообщение..."></textarea>
            <button class="send-btn" onclick="sendMessage()">📨 Отправить</button>
        </div>
        
        <div class="chat-header">
            <div class="chat-title">
                <h1>💬 Веб-чат</h1>
                <span class="online-status" id="onlineCount">0 онлайн</span>
            </div>
            <div class="user-info">
                <span class="username-display" id="currentUsername">Загрузка...</span>
                <button class="change-name-btn" onclick="changeUsername()">Сменить имя</button>
                <button class="toggle-users-btn" onclick="toggleUsers()">👥</button>
            </div>
        </div>
        
        <div class="chat-main" id="chatMain">
            <div class="users-sidebar" id="usersSidebar">
                <div class="users-header">👥 Участники (<span id="usersCount">0</span>)</div>
                <div class="users-list" id="usersList"><div>Подключение...</div></div>
            </div>
            <div class="messages-area">
                <div class="messages-container" id="messagesContainer"></div>
                <div class="typing-indicator" id="typingIndicator"></div>
            </div>
        </div>
    </div>
    <script>
        let ws = null, currentUser = null, sessionId = null, typingTimeout = null, isTyping = false, typingUsers = new Set();
        const messagesContainer = document.getElementById('messagesContainer');
        const messageInput = document.getElementById('messageInput');
        const typingIndicator = document.getElementById('typingIndicator');
        const currentUsernameSpan = document.getElementById('currentUsername');
        const onlineCountSpan = document.getElementById('onlineCount');
        const usersCountSpan = document.getElementById('usersCount');
        const usersList = document.getElementById('usersList');
        const usersSidebar = document.getElementById('usersSidebar');

        // Генерируем или получаем уникальный ID сессии
        function getSessionId() {
            let id = localStorage.getItem('chat_session_id');
            if (!id) {
                id = 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                localStorage.setItem('chat_session_id', id);
            }
            return id;
        }

        function toggleUsers() {
            usersSidebar.classList.toggle('show');
        }

        function connect(username, sessionId) {
            const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                console.log('Connected');
                ws.send(JSON.stringify({ username: username, session_id: sessionId }));
                setInterval(() => {
                    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' }));
                }, 30000);
            };
            ws.onmessage = (event) => { const data = JSON.parse(event.data); handleMessage(data); };
            ws.onerror = (error) => console.error('WebSocket error:', error);
            ws.onclose = () => { console.log('Disconnected'); showSystemMessage('Соединение потеряно. Переподключение...'); setTimeout(() => { if (currentUser) connect(currentUser, sessionId); }, 3000); };
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
            const textWithBreaks = escapeHtml(message.text).replace(/\\n/g, '<br>');
            messageDiv.innerHTML = `<div class="message-bubble"><div class="message-username">${escapeHtml(message.username)}</div><div class="message-text">${textWithBreaks}</div><div class="message-time">${formatTime(message.timestamp)}</div></div>`;
            messagesContainer.appendChild(messageDiv);
            scrollToBottom();
        }
        
        function showSystemMessage(text) { 
            const messageDiv = document.createElement('div'); 
            messageDiv.className = 'message system'; 
            messageDiv.innerHTML = `<div class="message-bubble">${escapeHtml(text)}</div>`; 
            messagesContainer.appendChild(messageDiv); 
            scrollToBottom(); 
        }
        
        function sendMessage() { 
            const text = messageInput.value;
            if (!text.trim() || !ws || ws.readyState !== WebSocket.OPEN) return; 
            ws.send(JSON.stringify({ type: 'message', text: text })); 
            messageInput.value = ''; 
            messageInput.style.height = 'auto';
            if (isTyping) { 
                ws.send(JSON.stringify({ type: 'typing', is_typing: false })); 
                isTyping = false; 
            } 
        }
        
        function handleKeyDown(event) {
            if (event.key === 'Enter') {
                event.preventDefault();
                const start = messageInput.selectionStart;
                const end = messageInput.selectionEnd;
                const value = messageInput.value;
                messageInput.value = value.substring(0, start) + '\\n' + value.substring(end);
                messageInput.selectionStart = messageInput.selectionEnd = start + 1;
                messageInput.dispatchEvent(new Event('input'));
            }
        }
        
        function handleKeyUp(event) {
            if (!isTyping && messageInput.value.length > 0 && ws && ws.readyState === WebSocket.OPEN) { 
                isTyping = true; 
                ws.send(JSON.stringify({ type: 'typing', is_typing: true })); 
            } 
            clearTimeout(typingTimeout); 
            typingTimeout = setTimeout(() => { 
                if (isTyping && ws && ws.readyState === WebSocket.OPEN) { 
                    isTyping = false; 
                    ws.send(JSON.stringify({ type: 'typing', is_typing: false })); 
                } 
            }, 1000);
        }
        
        function updateUsersList(users, count) { 
            usersCountSpan.textContent = count; 
            onlineCountSpan.textContent = `${count} онлайн`; 
            if (users.length === 0) { 
                usersList.innerHTML = '<div>Нет пользователей</div>'; 
                return; 
            } 
            usersList.innerHTML = users.map(user => {
                let sessionsHtml = '';
                if (user.sessions > 1) {
                    sessionsHtml = `<span class="user-sessions">📱 ${user.sessions} вкладки</span>`;
                }
                return `<div class="user-item"><div class="user-avatar"></div><div class="user-name">${escapeHtml(user.name)} ${user.name === currentUser ? '(Вы)' : ''}${sessionsHtml}</div></div>`;
            }).join(''); 
        }
        
        function updateOnlineCount(count) { 
            onlineCountSpan.textContent = `${count} онлайн`; 
            usersCountSpan.textContent = count; 
        }
        
        function updateTypingIndicator(username, isTypingUser) { 
            if (isTypingUser && username !== currentUser) typingUsers.add(username); 
            else typingUsers.delete(username); 
            if (typingUsers.size > 0) { 
                const names = Array.from(typingUsers); 
                let text = names.length === 1 ? `${names[0]} печатает...` : names.length === 2 ? `${names[0]} и ${names[1]} печатают...` : `${names.length} человек печатают...`; 
                typingIndicator.textContent = text; 
            } else typingIndicator.textContent = ''; 
        }
        
        function changeUsername() { 
            const newName = prompt('Введите новое имя (макс. 20 символов):', currentUser); 
            if (newName && newName.trim() && newName.trim() !== currentUser) { 
                currentUser = newName.trim().substring(0, 20); 
                currentUsernameSpan.textContent = currentUser; 
                localStorage.setItem('chat_username', currentUser);
                if (ws) ws.close(); 
                setTimeout(() => connect(currentUser, sessionId), 100); 
            } 
        }
        
        function escapeHtml(text) { 
            const div = document.createElement('div'); 
            div.textContent = text; 
            return div.innerHTML; 
        }
        
        function formatTime(timestamp) { 
            if (!timestamp) return ''; 
            return new Date(timestamp).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }); 
        }
        
        function scrollToBottom() { 
            messagesContainer.scrollTop = messagesContainer.scrollHeight; 
        }
        
        function autoResizeTextarea() {
            this.style.height = 'auto';
            const newHeight = Math.min(this.scrollHeight, 120);
            this.style.height = newHeight + 'px';
        }

        // Инициализация
        sessionId = getSessionId();
        const saved = localStorage.getItem('chat_username');
        if (saved) currentUser = saved;
        else { 
            currentUser = prompt('Ваше имя:', 'Гость') || `Гость_${Math.floor(Math.random() * 1000)}`; 
            localStorage.setItem('chat_username', currentUser); 
        }
        currentUsernameSpan.textContent = currentUser;
        connect(currentUser, sessionId);
        
        messageInput.addEventListener('input', autoResizeTextarea);
        messageInput.addEventListener('keydown', handleKeyDown);
        messageInput.addEventListener('keyup', handleKeyUp);
        
        messageInput.focus();
        
        document.addEventListener('click', function(event) {
            if (usersSidebar.classList.contains('show')) {
                const toggleBtn = document.querySelector('.toggle-users-btn');
                if (!usersSidebar.contains(event.target) && !toggleBtn.contains(event.target)) {
                    usersSidebar.classList.remove('show');
                }
            }
        });
    </script>
</body>
</html>'''

# --- HTTP и WebSocket обработчики ---
async def handle_index(request):
    return web.Response(text=HTML_PAGE, content_type='text/html')

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        msg = await ws.receive()
        if msg.type != web.WSMsgType.TEXT:
            await ws.close()
            return ws

        data = json.loads(msg.data)
        username = data.get('username', '').strip()
        session_id = data.get('session_id', '')
        
        if not username:
            username = f"Гость_{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6]}"
        username = username[:20]
        
        if not session_id:
            session_id = f"session_{datetime.now().timestamp()}"

        await chat_processor.register(ws, username, session_id)

        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await chat_processor.handle_message(ws, data)
                except json.JSONDecodeError:
                    pass
            elif msg.type == web.WSMsgType.ERROR:
                break
    except Exception as e:
        print(f"WebSocket handler error: {e}")
    finally:
        await chat_processor.unregister(ws)

    return ws

async def health_check(request):
    return web.Response(text="OK")

# --- Запуск приложения ---
app = web.Application()
app.router.add_get('/', handle_index)
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/healthz', health_check)

if __name__ == "__main__":
    print(f"🚀 Server starting on port {PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)
