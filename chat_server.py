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
        self.private_chats = {}  # {frozenset([user1, user2]): {'messages': [], 'participants': set()}}
        self.spam_filters = {}  # {'username': ['spammer1', 'spammer2']} - черный список спамеров для каждого пользователя
        self.spam_scores = {}  # {'username_spammer': score} - счетчик спама (внутренний)

    async def register(self, ws, username, session_id):
        # Сохраняем клиента с его сессией
        self.clients[ws] = {'username': username, 'session_id': session_id}
        connected_clients.add(ws)
        
        # Инициализируем спам-фильтр для пользователя если его нет
        if username not in self.spam_filters:
            self.spam_filters[username] = []
        
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
        """Отправка личного сообщения с проверкой спам-фильтра"""
        # Проверяем, не заблокирован ли отправитель получателем
        if to_username in self.spam_filters and from_username in self.spam_filters[to_username]:
            # Отправитель в черном списке, не отправляем сообщение
            # Уведомляем отправителя, что сообщение не доставлено
            for ws, client_data in self.clients.items():
                if client_data['username'] == from_username:
                    try:
                        if not ws.closed:
                            await ws.send_str(json.dumps({
                                'type': 'system',
                                'message': f'⚠️ Сообщение для {to_username} не доставлено: вы в черном списке получателя'
                            }))
                    except:
                        pass
            return False
        
        message = {
            'type': 'private_message',
            'from': from_username,
            'to': to_username,
            'text': text,
            'timestamp': datetime.now().isoformat(),
            'id': message_id
        }
        
        # Отправляем получателю
        delivered = False
        for ws, client_data in self.clients.items():
            if client_data['username'] == to_username:
                try:
                    if not ws.closed:
                        await ws.send_str(json.dumps(message))
                        delivered = True
                except:
                    pass
        
        # Отправляем отправителю подтверждение
        for ws, client_data in self.clients.items():
            if client_data['username'] == from_username:
                try:
                    if not ws.closed:
                        await ws.send_str(json.dumps(message))
                except:
                    pass
        
        return delivered

    async def send_private_message_parts(self, from_username, to_username, text):
        """Разбивает длинное сообщение на части и отправляет"""
        message_id = hashlib.md5(f"{from_username}{to_username}{datetime.now()}".encode()).hexdigest()[:8]
        
        if len(text) <= MAX_MESSAGE_LENGTH:
            return await self.send_private_message(from_username, to_username, text, message_id)
        
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

        elif msg_type == 'typing':
            await self.broadcast({
                'type': 'typing',
                'username': username,
                'is_typing': data.get('is_typing', False)
            })
            
        elif msg_type == 'mark_spam':
            # Отметить пользователя как спамера
            spammer = data.get('spammer')
            if spammer and spammer != username:
                if username not in self.spam_filters:
                    self.spam_filters[username] = []
                if spammer not in self.spam_filters[username]:
                    self.spam_filters[username].append(spammer)
                    
                    # Увеличиваем счетчик спама для спамера
                    spam_key = f"{spammer}"
                    self.spam_scores[spam_key] = self.spam_scores.get(spam_key, 0) + 1
                    
                    # Уведомляем пользователя
                    await ws.send_str(json.dumps({
                        'type': 'system',
                        'message': f'✅ Пользователь {spammer} добавлен в черный список. Сообщения от него не будут приходить.'
                    }))
                    
                    # Рассылаем обновленный список спамеров (только статистику)
                    await self.broadcast_spam_stats()
                    
        elif msg_type == 'unmark_spam':
            # Убрать отметку спама
            spammer = data.get('spammer')
            if spammer and username in self.spam_filters and spammer in self.spam_filters[username]:
                self.spam_filters[username].remove(spammer)
                await ws.send_str(json.dumps({
                    'type': 'system',
                    'message': f'✅ Пользователь {spammer} удален из черного списка. Сообщения от него снова будут приходить.'
                }))
                await self.broadcast_spam_stats()

        elif msg_type == 'get_spam_list':
            # Получить список спамеров для текущего пользователя
            spam_list = self.spam_filters.get(username, [])
            await ws.send_str(json.dumps({
                'type': 'spam_list',
                'spammers': spam_list
            }))
            
        elif msg_type == 'get_user_spam_status':
            # Получить статус спама для конкретного пользователя
            target_user = data.get('target_user')
            if target_user:
                is_spam = target_user in self.spam_filters.get(username, [])
                await ws.send_str(json.dumps({
                    'type': 'user_spam_status',
                    'user': target_user,
                    'is_spam': is_spam
                }))

        elif msg_type == 'ping':
            await ws.send_str(json.dumps({'type': 'pong'}))
            
    async def broadcast_spam_stats(self):
        """Транслирует статистику спама (только количество меток на пользователя)"""
        spam_stats = {}
        for user, spammers in self.spam_filters.items():
            for spammer in spammers:
                spam_stats[spammer] = spam_stats.get(spammer, 0) + 1
        
        await self.broadcast({
            'type': 'spam_stats',
            'stats': spam_stats
        })

chat_processor = ChatServer()

# --- Встроенный HTML (упрощенная и исправленная версия) ---
HTML_PAGE = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no, viewport-fit=cover">
    <meta name="theme-color" content="#0d1117">
    <title>💬 Веб-чат с защитой от спама</title>
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
            --danger: #da3633;
            --warning: #e3b341;
            --private-chat: #3a4a6e;
            --spam: #6e3a3a;
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
            max-width: 1400px;
            margin: 0 auto;
        }
        .input-area {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 10px 12px;
            display: flex;
            gap: 8px;
            flex-shrink: 0;
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
        }
        .chat-main {
            display: flex;
            flex: 1;
            overflow: hidden;
            min-height: 0;
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
            width: 280px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--border);
            display: none;
            flex-direction: column;
            overflow: hidden;
        }
        .users-sidebar.show {
            display: flex;
        }
        .users-header { 
            padding: 10px; 
            border-bottom: 1px solid var(--border); 
            font-weight: bold; 
            background: var(--bg-tertiary); 
        }
        .search-box {
            padding: 8px;
            border-bottom: 1px solid var(--border);
        }
        .search-input {
            width: 100%;
            padding: 8px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            border-radius: 20px;
            outline: none;
        }
        .search-input:focus {
            border-color: var(--accent);
        }
        .filter-buttons {
            padding: 8px;
            display: flex;
            gap: 8px;
            border-bottom: 1px solid var(--border);
        }
        .filter-btn {
            flex: 1;
            padding: 5px 8px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-secondary);
            border-radius: 15px;
            cursor: pointer;
            font-size: 0.75em;
        }
        .filter-btn.active {
            background: var(--accent);
            color: white;
        }
        .users-list { 
            flex: 1; 
            overflow-y: auto; 
            padding: 8px; 
        }
        .user-item { 
            padding: 8px 10px; 
            margin: 2px 0; 
            border-radius: 8px; 
            display: flex; 
            align-items: center; 
            gap: 8px; 
            cursor: pointer;
        }
        .user-item:hover { background: var(--bg-tertiary); }
        .user-item.spam { 
            background: var(--spam);
            opacity: 0.7;
        }
        .user-avatar { 
            width: 8px; 
            height: 8px; 
            border-radius: 50%; 
            background: var(--success); 
        }
        .user-avatar.spam { background: var(--danger); }
        .user-name { 
            word-break: break-word; 
            flex: 1; 
        }
        .private-badge, .spam-badge {
            font-size: 0.7em;
            padding: 2px 6px;
            border-radius: 10px;
            margin-left: 5px;
            cursor: pointer;
        }
        .private-badge { background: var(--private-chat); }
        .spam-badge { background: var(--danger); }
        .messages-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        .chat-tabs {
            display: flex;
            gap: 2px;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 5px 10px;
            overflow-x: auto;
            flex-shrink: 0;
        }
        .chat-tab {
            padding: 6px 12px;
            background: var(--bg-tertiary);
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            border-radius: 6px;
            white-space: nowrap;
        }
        .chat-tab.active {
            background: var(--accent);
            color: white;
        }
        .close-tab {
            margin-left: 8px;
            cursor: pointer;
            font-weight: bold;
        }
        .messages-container {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        .message { display: flex; }
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
            font-family: inherit;
            outline: none;
            resize: none;
            max-height: 120px;
            min-height: 40px;
        }
        .send-btn {
            background: var(--accent);
            color: white;
            border: none;
            padding: 0 20px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: bold;
        }
        .chat-title h1 { color: var(--accent); font-size: 1.1em; }
        .online-status { background: var(--success); color: white; padding: 3px 8px; border-radius: 20px; font-size: 0.7em; }
        .username-display { background: var(--bg-tertiary); padding: 3px 8px; border-radius: 20px; font-size: 0.8em; }
        .change-name-btn { background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 3px 8px; border-radius: 20px; cursor: pointer; font-size: 0.75em; }
        @media (max-width: 768px) {
            .users-sidebar { width: 100%; position: absolute; left: 0; right: 0; height: 100%; z-index: 1000; }
        }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="input-area">
            <textarea id="messageInput" class="message-input" placeholder="Введите сообщение..."></textarea>
            <button class="send-btn" id="sendButton">Отправить</button>
        </div>
        <div class="chat-header">
            <div class="chat-title">
                <h1>Веб-чат</h1>
                <span class="online-status" id="onlineCount">0 онлайн</span>
            </div>
            <div class="user-info">
                <span class="username-display" id="currentUsername">Загрузка...</span>
                <button class="change-name-btn" id="changeNameBtn">Сменить имя</button>
                <button class="toggle-users-btn" id="toggleUsersBtn">Участники</button>
            </div>
        </div>
        <div class="chat-main">
            <div class="users-sidebar" id="usersSidebar">
                <div class="users-header">Участники (<span id="usersCount">0</span>)</div>
                <div class="search-box">
                    <input type="text" id="userSearch" class="search-input" placeholder="Поиск...">
                </div>
                <div class="filter-buttons">
                    <button class="filter-btn active" data-filter="all">Все</button>
                    <button class="filter-btn" data-filter="spam">Спам</button>
                    <button class="filter-btn" data-filter="clean">Чистые</button>
                </div>
                <div class="users-list" id="usersList"></div>
            </div>
            <div class="messages-area">
                <div class="chat-tabs" id="chatTabs">
                    <button class="chat-tab active" data-chat="main">Общий чат</button>
                </div>
                <div class="messages-container" id="messagesContainer"></div>
                <div class="typing-indicator" id="typingIndicator"></div>
            </div>
        </div>
    </div>
    <script>
        var ws = null;
        var currentUser = null;
        var sessionId = null;
        var typingTimeout = null;
        var isTyping = false;
        var typingUsers = new Set();
        var currentChat = 'main';
        var privateChats = new Map();
        var allUsers = [];
        var currentUserFilter = 'all';
        var searchQuery = '';
        var spamStats = {};
        var spamList = [];
        
        var messagesContainer = document.getElementById('messagesContainer');
        var messageInput = document.getElementById('messageInput');
        var typingIndicator = document.getElementById('typingIndicator');
        var currentUsernameSpan = document.getElementById('currentUsername');
        var onlineCountSpan = document.getElementById('onlineCount');
        var usersCountSpan = document.getElementById('usersCount');
        var usersList = document.getElementById('usersList');
        var usersSidebar = document.getElementById('usersSidebar');
        var sendButton = document.getElementById('sendButton');
        var changeNameBtn = document.getElementById('changeNameBtn');
        var toggleUsersBtn = document.getElementById('toggleUsersBtn');
        var userSearch = document.getElementById('userSearch');
        
        window.messagesHistory = [];
        
        function getSessionId() {
            var id = localStorage.getItem('chat_session_id');
            if (!id) {
                id = 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                localStorage.setItem('chat_session_id', id);
            }
            return id;
        }
        
        function escapeHtml(text) {
            var div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function formatTime(timestamp) {
            if (!timestamp) return '';
            var d = new Date(timestamp);
            return d.getHours().toString().padStart(2,'0') + ':' + d.getMinutes().toString().padStart(2,'0');
        }
        
        function scrollToBottom() {
            messagesContainer.scrollTop = messagesContainer.scrollHeight;
        }
        
        function showSystemMessage(text) {
            if (currentChat !== 'main') return;
            var div = document.createElement('div');
            div.className = 'message system';
            div.innerHTML = '<div class="message-bubble">' + escapeHtml(text) + '</div>';
            messagesContainer.appendChild(div);
            scrollToBottom();
        }
        
        function addMessageToChat(message) {
            var div = document.createElement('div');
            div.className = 'message ' + (message.username === currentUser ? 'own' : '');
            var textWithBreaks = escapeHtml(message.text).replace(/\n/g, '<br>');
            div.innerHTML = '<div class="message-bubble"><div class="message-username">' + escapeHtml(message.username) + '</div><div class="message-text">' + textWithBreaks + '</div><div class="message-time">' + formatTime(message.timestamp) + '</div></div>';
            messagesContainer.appendChild(div);
            scrollToBottom();
        }
        
        function addPrivateMessageToChat(message, otherUser) {
            var div = document.createElement('div');
            var isFromMe = (message.from === currentUser);
            div.className = 'message ' + (isFromMe ? 'own' : '');
            var sender = isFromMe ? 'Вы' : message.from;
            var textWithBreaks = escapeHtml(message.text).replace(/\n/g, '<br>');
            div.innerHTML = '<div class="message-bubble"><div class="message-username">' + escapeHtml(sender) + '</div><div class="message-text">' + textWithBreaks + '</div><div class="message-time">' + formatTime(message.timestamp) + '</div></div>';
            messagesContainer.appendChild(div);
            scrollToBottom();
        }
        
        function showNotification(username) {
            var tabs = document.getElementById('chatTabs');
            var tab = Array.from(tabs.children).find(function(t) {
                return t.getAttribute('data-chat') === username;
            });
            if (tab && currentChat !== username) {
                tab.style.background = '#ff9800';
                setTimeout(function() {
                    if (currentChat !== username) tab.style.background = '';
                }, 1000);
            }
        }
        
        function addPrivateChatTab(username) {
            var tabsContainer = document.getElementById('chatTabs');
            var existing = Array.from(tabsContainer.children).find(function(tab) {
                return tab.getAttribute('data-chat') === username;
            });
            if (existing) return;
            var tab = document.createElement('button');
            tab.className = 'chat-tab private';
            tab.setAttribute('data-chat', username);
            tab.innerHTML = username + ' <span class="close-tab">✖</span>';
            tab.onclick = function(e) {
                if (e.target.className !== 'close-tab') window.switchChat(username);
            };
            var closeSpan = tab.querySelector('.close-tab');
            if (closeSpan) {
                closeSpan.onclick = function(e) {
                    e.stopPropagation();
                    window.closePrivateChat(username);
                };
            }
            tabsContainer.appendChild(tab);
        }
        
        window.closePrivateChat = function(username) {
            privateChats.delete(username);
            var tabsContainer = document.getElementById('chatTabs');
            var tab = Array.from(tabsContainer.children).find(function(t) {
                return t.getAttribute('data-chat') === username;
            });
            if (tab) tab.remove();
            if (currentChat === username) window.switchChat('main');
        };
        
        window.switchChat = function(chatId) {
            currentChat = chatId;
            var tabs = document.querySelectorAll('.chat-tab');
            tabs.forEach(function(tab) {
                var tabChat = tab.getAttribute('data-chat');
                if ((chatId === 'main' && tabChat === 'main') || (chatId !== 'main' && tabChat === chatId)) {
                    tab.classList.add('active');
                } else {
                    tab.classList.remove('active');
                }
            });
            messagesContainer.innerHTML = '';
            if (chatId === 'main') {
                window.messagesHistory.forEach(function(msg) {
                    if (msg.type === 'message') addMessageToChat(msg);
                });
            } else {
                var messages = privateChats.get(chatId) || [];
                messages.forEach(function(msg) {
                    addPrivateMessageToChat(msg, chatId);
                });
            }
            scrollToBottom();
        };
        
        window.startPrivateChat = function(username) {
            if (username === currentUser) {
                showSystemMessage('Нельзя начать чат с самим собой');
                return;
            }
            if (!privateChats.has(username)) {
                privateChats.set(username, []);
                addPrivateChatTab(username);
            }
            window.switchChat(username);
            usersSidebar.classList.remove('show');
        };
        
        function isSpamUser(username) {
            return spamList.indexOf(username) !== -1;
        }
        
        function filterUsers() {
            searchQuery = userSearch.value.toLowerCase();
            var filtered = [];
            for (var i = 0; i < allUsers.length; i++) {
                var user = allUsers[i];
                if (searchQuery && user.name.toLowerCase().indexOf(searchQuery) === -1) continue;
                if (currentUserFilter === 'spam' && !isSpamUser(user.name)) continue;
                if (currentUserFilter === 'clean' && isSpamUser(user.name)) continue;
                filtered.push(user);
            }
            filtered.sort(function(a, b) {
                if (a.name === currentUser) return -1;
                if (b.name === currentUser) return 1;
                return a.name.localeCompare(b.name);
            });
            if (filtered.length === 0) {
                usersList.innerHTML = '<div style="padding:10px;text-align:center;">Пользователи не найдены</div>';
                return;
            }
            var html = '';
            for (var i = 0; i < filtered.length; i++) {
                var user = filtered[i];
                var isCurrent = (user.name === currentUser);
                var isSpam = isSpamUser(user.name);
                var sessionsHtml = (user.sessions > 1) ? ' (' + user.sessions + ' вкладки)' : '';
                var spamCount = spamStats[user.name] || 0;
                var statsHtml = spamCount > 0 ? ' ⚠️' + spamCount : '';
                var onClick = isCurrent ? '' : ' onclick="startPrivateChat(\'' + escapeHtml(user.name) + '\')"';
                var onSpam = !isCurrent ? ' onclick="event.stopPropagation(); toggleSpam(\'' + escapeHtml(user.name) + '\')"' : '';
                var badgeText = isSpam ? 'Снять спам' : 'Спам';
                var badgeClass = isSpam ? 'spam-badge' : 'private-badge';
                html += '<div class="user-item ' + (isSpam ? 'spam' : '') + '"' + onClick + '>';
                html += '<div class="user-avatar ' + (isSpam ? 'spam' : '') + '"></div>';
                html += '<div class="user-name">' + escapeHtml(user.name) + (isCurrent ? ' (Вы)' : '') + sessionsHtml + statsHtml + '</div>';
                if (!isCurrent) html += '<span class="' + badgeClass + '"' + onSpam + '>' + badgeText + '</span>';
                html += '</div>';
            }
            usersList.innerHTML = html;
        }
        
        window.toggleSpam = function(username) {
            if (isSpamUser(username)) {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'unmark_spam', spammer: username }));
                }
                var idx = spamList.indexOf(username);
                if (idx > -1) spamList.splice(idx, 1);
                showSystemMessage(username + ' удален из черного списка');
            } else {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'mark_spam', spammer: username }));
                }
                spamList.push(username);
                showSystemMessage(username + ' отмечен как спам');
            }
            filterUsers();
        };
        
        window.setUserFilter = function(filter) {
            currentUserFilter = filter;
            var btns = document.querySelectorAll('.filter-btn');
            btns.forEach(function(btn) {
                btn.classList.remove('active');
                if (btn.getAttribute('data-filter') === filter) btn.classList.add('active');
            });
            filterUsers();
        };
        
        window.toggleUsers = function() {
            usersSidebar.classList.toggle('show');
            if (usersSidebar.classList.contains('show')) filterUsers();
        };
        
        window.sendMessage = function() {
            var text = messageInput.value;
            if (!text.trim() || !ws || ws.readyState !== WebSocket.OPEN) return;
            if (currentChat === 'main') {
                ws.send(JSON.stringify({ type: 'message', text: text }));
            } else {
                ws.send(JSON.stringify({ type: 'private_message', to: currentChat, text: text }));
            }
            messageInput.value = '';
            if (isTyping) {
                ws.send(JSON.stringify({ type: 'typing', is_typing: false }));
                isTyping = false;
            }
        };
        
        function updateTypingIndicator(username, isTypingUser) {
            if (currentChat !== 'main') return;
            if (isTypingUser && username !== currentUser) typingUsers.add(username);
            else typingUsers.delete(username);
            if (typingUsers.size > 0) {
                var names = Array.from(typingUsers);
                var text = '';
                if (names.length === 1) text = names[0] + ' печатает...';
                else if (names.length === 2) text = names[0] + ' и ' + names[1] + ' печатают...';
                else text = names.length + ' человек печатают...';
                typingIndicator.textContent = text;
            } else {
                typingIndicator.textContent = '';
            }
        }
        
        function handlePrivateMessage(message) {
            var isFromMe = (message.from === currentUser);
            var otherUser = isFromMe ? message.to : message.from;
            if (!isFromMe && spamList.indexOf(message.from) !== -1) return;
            if (!privateChats.has(otherUser)) {
                privateChats.set(otherUser, []);
                addPrivateChatTab(otherUser);
            }
            privateChats.get(otherUser).push(message);
            if (currentChat === otherUser) {
                addPrivateMessageToChat(message, otherUser);
            } else if (!isFromMe) {
                showNotification(otherUser);
            }
        }
        
        function updateUsersList(users, count) {
            allUsers = users;
            usersCountSpan.textContent = count;
            onlineCountSpan.textContent = count + ' онлайн';
            filterUsers();
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
                    showSystemMessage(data.message);
                    if (data.users_count) onlineCountSpan.textContent = data.users_count + ' онлайн';
                    break;
                case 'users_list':
                    updateUsersList(data.users, data.count);
                    break;
                case 'typing':
                    updateTypingIndicator(data.username, data.is_typing);
                    break;
                case 'spam_list':
                    spamList = data.spammers || [];
                    filterUsers();
                    break;
                case 'spam_stats':
                    spamStats = data.stats || {};
                    filterUsers();
                    break;
            }
        }
        
        function connect(username, sessionId) {
            var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            var url = protocol + '//' + window.location.host + '/ws';
            ws = new WebSocket(url);
            ws.onopen = function() {
                ws.send(JSON.stringify({ username: username, session_id: sessionId }));
                setInterval(function() {
                    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' }));
                }, 30000);
                setTimeout(function() {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'get_spam_list' }));
                    }
                }, 1000);
            };
            ws.onmessage = function(event) {
                var data = JSON.parse(event.data);
                handleMessage(data);
            };
            ws.onclose = function() {
                showSystemMessage('Соединение потеряно. Переподключение...');
                setTimeout(function() { if (currentUser) connect(currentUser, sessionId); }, 3000);
            };
        }
        
        window.changeUsername = function() {
            var newName = prompt('Введите новое имя (макс. 20 символов):', currentUser);
            if (newName && newName.trim() && newName.trim() !== currentUser) {
                currentUser = newName.trim().substring(0, 20);
                currentUsernameSpan.textContent = currentUser;
                localStorage.setItem('chat_username', currentUser);
                if (ws) ws.close();
                setTimeout(function() { connect(currentUser, sessionId); }, 100);
            }
        };
        
        sessionId = getSessionId();
        var saved = localStorage.getItem('chat_username');
        if (saved) {
            currentUser = saved;
        } else {
            currentUser = prompt('Ваше имя:', 'Гость') || 'Гость_' + Math.floor(Math.random() * 1000);
            localStorage.setItem('chat_username', currentUser);
        }
        currentUsernameSpan.textContent = currentUser;
        
        var origAddMsg = addMessageToChat;
        window.addMessageToChat = function(message) {
            window.messagesHistory.push(message);
            if (window.messagesHistory.length > 100) window.messagesHistory.shift();
            origAddMsg(message);
        };
        
        connect(currentUser, sessionId);
        
        sendButton.onclick = window.sendMessage;
        changeNameBtn.onclick = window.changeUsername;
        toggleUsersBtn.onclick = window.toggleUsers;
        userSearch.onkeyup = filterUsers;
        
        messageInput.onkeydown = function(e) {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                window.sendMessage();
            }
        };
        
        var filterBtns = document.querySelectorAll('.filter-btn');
        filterBtns.forEach(function(btn) {
            btn.onclick = function() {
                window.setUserFilter(this.getAttribute('data-filter'));
            };
        });
        
        var mainTab = document.querySelector('.chat-tab[data-chat="main"]');
        if (mainTab) {
            mainTab.onclick = function() { window.switchChat('main'); };
        }
        
        messageInput.focus();
        
        document.onclick = function(event) {
            if (usersSidebar.classList.contains('show')) {
                if (!usersSidebar.contains(event.target) && event.target !== toggleUsersBtn) {
                    usersSidebar.classList.remove('show');
                }
            }
        };
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
