import asyncio
import json
import os
from datetime import datetime
import hashlib
from aiohttp import web

# === НАСТРОЙКИ ===
PORT = int(os.environ.get("PORT", 8080))
MAX_MESSAGE_LENGTH = 1600  # Максимальная длина одного сообщения
# =================

# --- Хранилище данных чата ---
messages_history = []
MAX_HISTORY = 100
connected_clients = set()

class ChatServer:
    def __init__(self):
        self.clients = {}  # {websocket: {'username': username, 'session_id': session_id}}
        self.user_sessions = {}  # {'username_session': count}
        self.unread_messages = {}  # {username: {from_user: count}}

    async def register(self, ws, username, session_id):
        # Сохраняем клиента с его сессией
        self.clients[ws] = {'username': username, 'session_id': session_id}
        connected_clients.add(ws)
        
        # Инициализируем счетчик непрочитанных сообщений
        if username not in self.unread_messages:
            self.unread_messages[username] = {}
        
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
        
        # Отправляем пользователю его непрочитанные сообщения
        await self.send_unread_messages(ws, username)

    async def send_unread_messages(self, ws, username):
        """Отправляет пользователю список непрочитанных сообщений"""
        unread = self.unread_messages.get(username, {})
        if unread:
            await ws.send_str(json.dumps({
                'type': 'unread_update',
                'unread': unread
            }))

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

    async def broadcast(self, message, exclude_ws=None):
        if not connected_clients:
            return
        message_json = json.dumps(message)
        for client in list(connected_clients):
            if exclude_ws and client == exclude_ws:
                continue
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

    async def send_private_message(self, from_username, to_username, text, message_id):
        """Отправка личного сообщения"""
        message = {
            'type': 'private_message',
            'from': from_username,
            'to': to_username,
            'text': text,
            'timestamp': datetime.now().isoformat(),
            'id': message_id
        }
        
        # Увеличиваем счетчик непрочитанных для получателя
        if to_username not in self.unread_messages:
            self.unread_messages[to_username] = {}
        self.unread_messages[to_username][from_username] = self.unread_messages[to_username].get(from_username, 0) + 1
        
        # Отправляем получателю
        sent_to_receiver = False
        for ws, client_data in self.clients.items():
            if client_data['username'] == to_username:
                try:
                    if not ws.closed:
                        await ws.send_str(json.dumps(message))
                        # Сбрасываем счетчик при активном чате
                        if to_username in self.unread_messages and from_username in self.unread_messages[to_username]:
                            del self.unread_messages[to_username][from_username]
                        sent_to_receiver = True
                except:
                    pass
        
        # Если получатель не в сети, оставляем счетчик
        if not sent_to_receiver:
            # Уведомляем всех клиентов получателя о непрочитанных
            for ws, client_data in self.clients.items():
                if client_data['username'] == to_username:
                    await self.send_unread_messages(ws, to_username)
        
        # Отправляем отправителю
        for ws, client_data in self.clients.items():
            if client_data['username'] == from_username:
                try:
                    if not ws.closed:
                        await ws.send_str(json.dumps(message))
                except:
                    pass
        
        # Обновляем список пользователей для всех
        await self.broadcast_users_list()

    async def mark_as_read(self, username, from_user):
        """Отмечает сообщения как прочитанные"""
        if username in self.unread_messages and from_user in self.unread_messages[username]:
            del self.unread_messages[username][from_user]
            # Уведомляем клиента об обновлении
            for ws, client_data in self.clients.items():
                if client_data['username'] == username:
                    await self.send_unread_messages(ws, username)
                    break

    async def send_private_message_parts(self, from_username, to_username, text):
        """Разбивает длинное сообщение на части и отправляет"""
        message_id = hashlib.md5(f"{from_username}{to_username}{datetime.now()}".encode()).hexdigest()[:8]
        
        if len(text) <= MAX_MESSAGE_LENGTH:
            await self.send_private_message(from_username, to_username, text, message_id)
            return
        
        # Разбиваем сообщение на части
        parts = []
        for i in range(0, len(text), MAX_MESSAGE_LENGTH):
            part = text[i:i+MAX_MESSAGE_LENGTH]
            parts.append(part)
        
        # Отправляем каждую часть
        for idx, part in enumerate(parts, 1):
            part_text = f"[{idx}/{len(parts)}] {part}" if len(parts) > 1 else part
            await self.send_private_message(from_username, to_username, part_text, f"{message_id}_{idx}")

    async def handle_message(self, ws, data):
        if ws not in self.clients:
            return
        
        client_data = self.clients[ws]
        username = client_data['username']

        msg_type = data.get('type', 'message')

        if msg_type == 'message':
            text = data.get('text', '')
            message_id = hashlib.md5(f"{username}{datetime.now()}".encode()).hexdigest()[:8]
            
            # Разбиваем длинное сообщение на части для общего чата
            if len(text) <= MAX_MESSAGE_LENGTH:
                message = {
                    'type': 'message',
                    'username': username,
                    'text': text,
                    'timestamp': datetime.now().isoformat(),
                    'id': message_id
                }
                messages_history.append(message)
                if len(messages_history) > MAX_HISTORY:
                    messages_history.pop(0)
                await self.broadcast(message)
            else:
                # Разбиваем на части
                parts = []
                for i in range(0, len(text), MAX_MESSAGE_LENGTH):
                    part = text[i:i+MAX_MESSAGE_LENGTH]
                    parts.append(part)
                
                for idx, part in enumerate(parts, 1):
                    part_text = f"[{idx}/{len(parts)}] {part}" if len(parts) > 1 else part
                    message = {
                        'type': 'message',
                        'username': username,
                        'text': part_text,
                        'timestamp': datetime.now().isoformat(),
                        'id': f"{message_id}_{idx}"
                    }
                    messages_history.append(message)
                    if len(messages_history) > MAX_HISTORY:
                        messages_history.pop(0)
                    await self.broadcast(message)

        elif msg_type == 'private_message':
            to_username = data.get('to')
            text = data.get('text', '')
            if to_username:
                await self.send_private_message_parts(username, to_username, text)

        elif msg_type == 'mark_read':
            from_user = data.get('from_user')
            if from_user:
                await self.mark_as_read(username, from_user)

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
            --private-chat: #3a4a6e;
            --notification: #ff9800;
            --unread-badge: #f44336;
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
        
        /* Кнопки навигации */
        .nav-buttons {
            display: flex;
            gap: 8px;
            background: var(--bg-secondary);
            padding: 8px 12px;
            border-bottom: 1px solid var(--border);
            flex-shrink: 0;
        }
        
        .nav-btn {
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 8px 16px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.9em;
            position: relative;
            flex: 1;
            text-align: center;
        }
        
        .nav-btn.active {
            background: var(--accent);
            border-color: var(--accent);
        }
        
        .badge {
            position: absolute;
            top: -5px;
            right: -5px;
            background: var(--unread-badge);
            color: white;
            border-radius: 50%;
            padding: 2px 6px;
            font-size: 0.7em;
            min-width: 18px;
            text-align: center;
        }
        
        /* Список диалогов */
        .dialogs-list {
            flex: 1;
            overflow-y: auto;
            padding: 8px;
        }
        
        .dialog-item {
            padding: 12px;
            margin: 4px 0;
            background: var(--bg-tertiary);
            border-radius: 12px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background 0.2s;
        }
        
        .dialog-item:hover {
            background: var(--bg-secondary);
        }
        
        .dialog-info {
            flex: 1;
        }
        
        .dialog-name {
            font-weight: bold;
            font-size: 1em;
        }
        
        .dialog-preview {
            font-size: 0.8em;
            color: var(--text-secondary);
            margin-top: 4px;
        }
        
        .unread-count {
            background: var(--unread-badge);
            color: white;
            border-radius: 50%;
            padding: 4px 8px;
            font-size: 0.75em;
            min-width: 24px;
            text-align: center;
            font-weight: bold;
        }
        
        /* Список пользователей */
        .users-sidebar, .dialogs-sidebar {
            background: var(--bg-secondary);
            display: none;
            flex-direction: column;
            overflow: hidden;
            width: 100%;
        }
        
        .users-sidebar.show, .dialogs-sidebar.show {
            display: flex;
        }
        
        .sidebar-header {
            padding: 12px;
            border-bottom: 1px solid var(--border);
            font-weight: bold;
            background: var(--bg-tertiary);
            font-size: 0.9em;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .close-sidebar {
            background: none;
            border: none;
            color: var(--text-secondary);
            font-size: 1.2em;
            cursor: pointer;
            padding: 0 8px;
        }
        
        .users-list, .dialogs-list-scroll {
            flex: 1;
            overflow-y: auto;
            padding: 8px;
        }
        
        .user-item {
            padding: 10px;
            margin: 4px 0;
            background: var(--bg-tertiary);
            border-radius: 8px;
            display: flex;
            align-items: center;
            gap: 10px;
            cursor: pointer;
            transition: background 0.2s;
        }
        
        .user-item:active { background: var(--bg-secondary); }
        .user-avatar { width: 10px; height: 10px; border-radius: 50%; background: var(--success); flex-shrink: 0; }
        .user-name { word-break: break-word; flex: 1; font-size: 0.9em; }
        .user-sessions { font-size: 0.7em; color: var(--text-secondary); margin-left: 4px; }
        
        .messages-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        
        .current-chat-header {
            padding: 8px 12px;
            background: var(--bg-tertiary);
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-shrink: 0;
        }
        
        .back-btn {
            background: none;
            border: none;
            color: var(--accent);
            font-size: 1.2em;
            cursor: pointer;
            padding: 4px 8px;
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
        
        .char-counter {
            font-size: 0.7em;
            color: var(--text-secondary);
            padding: 5px;
            text-align: right;
        }
        
        .char-counter.warning { color: orange; }
        .char-counter.danger { color: red; }
        
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
        
        @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
        
        @media (max-width: 768px) {
            .message-bubble { max-width: 85%; }
            .nav-btn { padding: 6px 12px; font-size: 0.85em; }
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
            </div>
        </div>
        
        <div class="nav-buttons">
            <button class="nav-btn" id="mainChatBtn" onclick="showMainChat()">💬 Общий чат</button>
            <button class="nav-btn" id="dialogsBtn" onclick="showDialogs()">💌 Диалоги <span id="totalUnread" class="badge" style="display: none;">0</span></button>
            <button class="nav-btn" id="usersBtn" onclick="showUsers()">👥 Участники</button>
        </div>
        
        <div class="chat-main">
            <!-- Список диалогов -->
            <div class="dialogs-sidebar" id="dialogsSidebar">
                <div class="sidebar-header">
                    💌 Личные сообщения
                    <button class="close-sidebar" onclick="closeSidebars()">✖</button>
                </div>
                <div class="dialogs-list" id="dialogsList"></div>
            </div>
            
            <!-- Список пользователей -->
            <div class="users-sidebar" id="usersSidebar">
                <div class="sidebar-header">
                    👥 Участники (<span id="usersCount">0</span>)
                    <button class="close-sidebar" onclick="closeSidebars()">✖</button>
                </div>
                <div class="users-list" id="usersList"><div>Подключение...</div></div>
            </div>
            
            <!-- Область сообщений -->
            <div class="messages-area" id="messagesArea">
                <div class="current-chat-header" id="currentChatHeader" style="display: none;">
                    <button class="back-btn" onclick="backToDialogs()">← Назад</button>
                    <span id="currentChatName"></span>
                    <div style="width: 30px;"></div>
                </div>
                <div class="messages-container" id="messagesContainer"></div>
                <div class="char-counter" id="charCounter">0/1600</div>
                <div class="typing-indicator" id="typingIndicator"></div>
            </div>
        </div>
    </div>
    <script>
        let ws = null, currentUser = null, sessionId = null, typingTimeout = null, isTyping = false;
        let currentChat = 'main'; // 'main' или имя пользователя для личного чата
        let privateMessages = new Map(); // {username: messages[]}
        let unreadMessages = new Map(); // {username: count}
        let dialogsList = []; // Список диалогов
        
        const messagesContainer = document.getElementById('messagesContainer');
        const messageInput = document.getElementById('messageInput');
        const typingIndicator = document.getElementById('typingIndicator');
        const currentUsernameSpan = document.getElementById('currentUsername');
        const onlineCountSpan = document.getElementById('onlineCount');
        const usersCountSpan = document.getElementById('usersCount');
        const usersList = document.getElementById('usersList');
        const usersSidebar = document.getElementById('usersSidebar');
        const dialogsSidebar = document.getElementById('dialogsSidebar');
        const dialogsListDiv = document.getElementById('dialogsList');
        const currentChatHeader = document.getElementById('currentChatHeader');
        const currentChatName = document.getElementById('currentChatName');
        const messagesArea = document.getElementById('messagesArea');
        const charCounter = document.getElementById('charCounter');
        const totalUnreadSpan = document.getElementById('totalUnread');
        
        // Генерация ID сессии
        function getSessionId() {
            let id = localStorage.getItem('chat_session_id');
            if (!id) {
                id = 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                localStorage.setItem('chat_session_id', id);
            }
            return id;
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
            ws.onclose = () => { 
                console.log('Disconnected'); 
                showSystemMessage('Соединение потеряно. Переподключение...'); 
                setTimeout(() => { if (currentUser) connect(currentUser, sessionId); }, 3000); 
            };
        }
        
        function handleMessage(data) {
            switch(data.type) {
                case 'message': 
                    if (currentChat === 'main') addMessageToChat(data);
                    break;
                case 'private_message':
                    handlePrivateMessage(data);
                    break;
                case 'system': 
                    if (currentChat === 'main') showSystemMessage(data.message);
                    if (data.users_count) updateOnlineCount(data.users_count);
                    break;
                case 'users_list':
                    updateUsersList(data.users, data.count);
                    break;
                case 'typing':
                    if (currentChat === 'main') updateTypingIndicator(data.username, data.is_typing);
                    break;
                case 'unread_update':
                    updateUnreadMessages(data.unread);
                    break;
            }
        }
        
        function updateUnreadMessages(unread) {
            unreadMessages.clear();
            for (const [from, count] of Object.entries(unread)) {
                unreadMessages.set(from, count);
                // Добавляем в список диалогов если его там нет
                if (!dialogsList.includes(from) && from !== currentUser) {
                    dialogsList.push(from);
                }
            }
            updateDialogsList();
            updateTotalUnreadBadge();
        }
        
        function updateTotalUnreadBadge() {
            let total = 0;
            for (let count of unreadMessages.values()) {
                total += count;
            }
            if (total > 0) {
                totalUnreadSpan.textContent = total;
                totalUnreadSpan.style.display = 'inline-block';
            } else {
                totalUnreadSpan.style.display = 'none';
            }
        }
        
        function handlePrivateMessage(message) {
            const isFromMe = message.from === currentUser;
            const otherUser = isFromMe ? message.to : message.from;
            
            // Сохраняем сообщение
            if (!privateMessages.has(otherUser)) {
                privateMessages.set(otherUser, []);
                if (!dialogsList.includes(otherUser)) {
                    dialogsList.push(otherUser);
                }
            }
            privateMessages.get(otherUser).push(message);
            
            // Если не в этом чате и не от меня, увеличиваем счетчик
            if (currentChat !== otherUser && !isFromMe) {
                const count = unreadMessages.get(otherUser) || 0;
                unreadMessages.set(otherUser, count + 1);
                updateDialogsList();
                updateTotalUnreadBadge();
            }
            
            // Если открыт этот чат, показываем сообщение
            if (currentChat === otherUser) {
                addPrivateMessageToChat(message);
                // Отмечаем как прочитанное
                if (!isFromMe) {
                    markAsRead(otherUser);
                }
            } else if (!isFromMe) {
                // Показываем уведомление в списке диалогов
                updateDialogsList();
            }
        }
        
        function markAsRead(username) {
            if (unreadMessages.has(username)) {
                unreadMessages.delete(username);
                updateDialogsList();
                updateTotalUnreadBadge();
                // Отправляем на сервер отметку о прочтении
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'mark_read', from_user: username }));
                }
            }
        }
        
        function updateDialogsList() {
            if (dialogsList.length === 0) {
                dialogsListDiv.innerHTML = '<div style="padding: 20px; text-align: center; color: var(--text-secondary);">Нет диалогов</div>';
                return;
            }
            
            const dialogs = [...dialogsList].sort((a, b) => {
                // Сортируем по наличию непрочитанных
                const unreadA = unreadMessages.has(a) ? 1 : 0;
                const unreadB = unreadMessages.has(b) ? 1 : 0;
                return unreadB - unreadA;
            });
            
            dialogsListDiv.innerHTML = dialogs.map(username => {
                const messages = privateMessages.get(username) || [];
                const lastMessage = messages[messages.length - 1];
                const preview = lastMessage ? (lastMessage.text.length > 50 ? lastMessage.text.substring(0, 50) + '...' : lastMessage.text) : 'Нет сообщений';
                const unreadCount = unreadMessages.get(username) || 0;
                const unreadBadge = unreadCount > 0 ? `<div class="unread-count">${unreadCount}</div>` : '';
                
                return `
                    <div class="dialog-item" onclick="openPrivateChat('${username.replace(/'/g, "\\'")}')">
                        <div class="dialog-info">
                            <div class="dialog-name">${escapeHtml(username)}</div>
                            <div class="dialog-preview">${escapeHtml(preview)}</div>
                        </div>
                        ${unreadBadge}
                    </div>
                `;
            }).join('');
        }
        
        function showDialogs() {
            currentChat = 'dialogs_list';
            messagesArea.style.display = 'none';
            usersSidebar.classList.remove('show');
            dialogsSidebar.classList.add('show');
            updateDialogsList();
        }
        
        function showUsers() {
            currentChat = 'users_list';
            messagesArea.style.display = 'none';
            dialogsSidebar.classList.remove('show');
            usersSidebar.classList.add('show');
        }
        
        function showMainChat() {
            currentChat = 'main';
            messagesArea.style.display = 'flex';
            dialogsSidebar.classList.remove('show');
            usersSidebar.classList.remove('show');
            currentChatHeader.style.display = 'none';
            
            // Очищаем и показываем общие сообщения
            messagesContainer.innerHTML = '';
            window.messagesHistory.forEach(msg => {
                if (msg.type === 'message') {
                    addMessageToChat(msg);
                }
            });
            scrollToBottom();
        }
        
        function openPrivateChat(username) {
            currentChat = username;
            messagesArea.style.display = 'flex';
            dialogsSidebar.classList.remove('show');
            usersSidebar.classList.remove('show');
            currentChatHeader.style.display = 'flex';
            currentChatName.textContent = username;
            
            // Отмечаем как прочитанное
            markAsRead(username);
            
            // Показываем сообщения
            messagesContainer.innerHTML = '';
            const messages = privateMessages.get(username) || [];
            messages.forEach(msg => {
                addPrivateMessageToChat(msg);
            });
            scrollToBottom();
            messageInput.focus();
        }
        
        function backToDialogs() {
            showDialogs();
        }
        
        function closeSidebars() {
            if (currentChat === 'dialogs_list' || currentChat === 'users_list') {
                showMainChat();
            } else {
                dialogsSidebar.classList.remove('show');
                usersSidebar.classList.remove('show');
            }
        }
        
        function addPrivateMessageToChat(message) {
            const messageDiv = document.createElement('div');
            const isFromMe = message.from === currentUser;
            messageDiv.className = `message ${isFromMe ? 'own' : ''}`;
            const sender = isFromMe ? 'Вы' : message.from;
            const textWithBreaks = escapeHtml(message.text).replace(/\\n/g, '<br>');
            messageDiv.innerHTML = `<div class="message-bubble"><div class="message-username">${escapeHtml(sender)}</div><div class="message-text">${textWithBreaks}</div><div class="message-time">${formatTime(message.timestamp)}</div></div>`;
            messagesContainer.appendChild(messageDiv);
            scrollToBottom();
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
            if (currentChat !== 'main') return;
            const messageDiv = document.createElement('div');
            messageDiv.className = 'message system';
            messageDiv.innerHTML = `<div class="message-bubble">${escapeHtml(text)}</div>`;
            messagesContainer.appendChild(messageDiv);
            scrollToBottom();
        }
        
        function sendMessage() {
            const text = messageInput.value;
            if (!text.trim() || !ws || ws.readyState !== WebSocket.OPEN) return;
            
            if (currentChat === 'main') {
                ws.send(JSON.stringify({ type: 'message', text: text }));
            } else if (currentChat !== 'dialogs_list' && currentChat !== 'users_list') {
                ws.send(JSON.stringify({
                    type: 'private_message',
                    to: currentChat,
                    text: text
                }));
            }
            
            messageInput.value = '';
            messageInput.style.height = 'auto';
            updateCharCounter();
            if (isTyping) {
                ws.send(JSON.stringify({ type: 'typing', is_typing: false }));
                isTyping = false;
            }
        }
        
        function updateCharCounter() {
            const length = messageInput.value.length;
            charCounter.textContent = `${length}/1600`;
            if (length > 1400) {
                charCounter.className = 'char-counter warning';
            } else if (length > 1600) {
                charCounter.className = 'char-counter danger';
            } else {
                charCounter.className = 'char-counter';
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
            updateCharCounter();
            if (!isTyping && messageInput.value.length > 0 && ws && ws.readyState === WebSocket.OPEN && currentChat !== 'dialogs_list' && currentChat !== 'users_list') {
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
                const isCurrent = user.name === currentUser;
                const onClick = isCurrent ? '' : `onclick="startPrivateChat('${escapeHtml(user.name)}')"`;
                return `<div class="user-item" ${onClick}><div class="user-avatar"></div><div class="user-name">${escapeHtml(user.name)} ${isCurrent ? '(Вы)' : ''}${sessionsHtml}</div></div>`;
            }).join('');
        }
        
        function startPrivateChat(username) {
            if (username === currentUser) {
                showSystemMessage('Нельзя начать чат с самим собой');
                return;
            }
            if (!privateMessages.has(username)) {
                privateMessages.set(username, []);
                if (!dialogsList.includes(username)) {
                    dialogsList.push(username);
                }
            }
            openPrivateChat(username);
        }
        
        function updateOnlineCount(count) {
            onlineCountSpan.textContent = `${count} онлайн`;
            usersCountSpan.textContent = count;
        }
        
        function updateTypingIndicator(username, isTypingUser) {
            if (currentChat !== 'main') return;
            // Простая реализация (можно расширить)
            if (isTypingUser && username !== currentUser) {
                typingIndicator.textContent = `${username} печатает...`;
                setTimeout(() => {
                    if (typingIndicator.textContent === `${username} печатает...`) {
                        typingIndicator.textContent = '';
                    }
                }, 1500);
            }
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
        
        // Сохраняем историю сообщений
        window.messagesHistory = [];
        const originalAddMessage = addMessageToChat;
        window.addMessageToChat = function(message) {
            window.messagesHistory.push(message);
            if (window.messagesHistory.length > 100) window.messagesHistory.shift();
            originalAddMessage(message);
        };
        
        connect(currentUser, sessionId);
        
        messageInput.addEventListener('input', (e) => {
            autoResizeTextarea.call(messageInput);
            updateCharCounter();
        });
        messageInput.addEventListener('keydown', handleKeyDown);
        messageInput.addEventListener('keyup', handleKeyUp);
        
        // Показываем общий чат по умолчанию
        showMainChat();
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
