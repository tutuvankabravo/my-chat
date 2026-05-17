import asyncio
import json
import os
from datetime import datetime
import hashlib
from aiohttp import web

# === НАСТРОЙКИ ===
PORT = int(os.environ.get("PORT", 8080))
MAX_MESSAGE_LENGTH = 1600
# =================

messages_history = []
MAX_HISTORY = 100
connected_clients = set()

class ChatServer:
    def __init__(self):
        self.clients = {}
        self.user_sessions = {}
        self.private_chats = {}
        self.spam_filters = {}
        self.spam_scores = {}

    def is_nickname_taken(self, username, exclude_ws=None):
        for ws, client_data in self.clients.items():
            if exclude_ws and ws == exclude_ws:
                continue
            if client_data['username'] == username:
                return True
        return False

    def generate_unique_nickname(self, base_nickname):
        if not self.is_nickname_taken(base_nickname):
            return base_nickname
        counter = 1
        while self.is_nickname_taken(f"{base_nickname}{counter}"):
            counter += 1
        return f"{base_nickname}{counter}"

    async def register(self, ws, username, session_id):
        original_username = username
        if self.is_nickname_taken(username):
            username = self.generate_unique_nickname(username)
            await ws.send_str(json.dumps({
                'type': 'system',
                'message': f'⚠️ Имя "{original_username}" уже занято. Вы вошли как "{username}"'
            }))
        
        self.clients[ws] = {'username': username, 'session_id': session_id}
        connected_clients.add(ws)
        
        if username not in self.spam_filters:
            self.spam_filters[username] = []
        
        session_key = f"{username}_{session_id}"
        self.user_sessions[session_key] = self.user_sessions.get(session_key, 0) + 1

        for msg in messages_history[-50:]:
            try:
                await ws.send_str(json.dumps(msg))
            except:
                pass

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
            
            self.user_sessions[session_key] = self.user_sessions.get(session_key, 1) - 1
            if self.user_sessions[session_key] <= 0:
                del self.user_sessions[session_key]
                await self.broadcast({
                    'type': 'system',
                    'message': f'👋 {username} покинул чат',
                    'users_count': len(self.get_unique_users())
                })
            
            await self.broadcast_users_list()

    def get_unique_users(self):
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
        if to_username in self.spam_filters and from_username in self.spam_filters[to_username]:
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
        
        delivered = False
        for ws, client_data in self.clients.items():
            if client_data['username'] == to_username:
                try:
                    if not ws.closed:
                        await ws.send_str(json.dumps(message))
                        delivered = True
                except:
                    pass
        
        for ws, client_data in self.clients.items():
            if client_data['username'] == from_username:
                try:
                    if not ws.closed:
                        await ws.send_str(json.dumps(message))
                except:
                    pass
        
        return delivered

    async def send_private_message_parts(self, from_username, to_username, text):
        message_id = hashlib.md5(f"{from_username}{to_username}{datetime.now()}".encode()).hexdigest()[:8]
        if len(text) <= MAX_MESSAGE_LENGTH:
            return await self.send_private_message(from_username, to_username, text, message_id)
        
        parts = []
        for i in range(0, len(text), MAX_MESSAGE_LENGTH):
            part = text[i:i+MAX_MESSAGE_LENGTH]
            parts.append(part)
        
        for idx, part in enumerate(parts, 1):
            part_text = f"[{idx}/{len(parts)}] {part}" if len(parts) > 1 else part
            await self.send_private_message(from_username, to_username, part_text, f"{message_id}_{idx}")

    async def change_username(self, ws, old_username, new_username):
        if not new_username or not new_username.strip():
            await ws.send_str(json.dumps({
                'type': 'system',
                'message': '❌ Имя не может быть пустым'
            }))
            return False
        
        new_username = new_username.strip()[:20]
        
        if self.is_nickname_taken(new_username, exclude_ws=ws):
            await ws.send_str(json.dumps({
                'type': 'system',
                'message': f'❌ Имя "{new_username}" уже занято. Выберите другое'
            }))
            return False
        
        self.clients[ws]['username'] = new_username
        
        if old_username in self.spam_filters:
            self.spam_filters[new_username] = self.spam_filters.pop(old_username)
        
        await self.broadcast({
            'type': 'system',
            'message': f'✏️ {old_username} сменил имя на {new_username}'
        })
        
        await self.broadcast_users_list()
        
        await ws.send_str(json.dumps({
            'type': 'system',
            'message': f'✅ Вы успешно сменили имя на {new_username}'
        }))
        return True

    async def handle_message(self, ws, data):
        if ws not in self.clients:
            return
        
        client_data = self.clients[ws]
        username = client_data['username']
        msg_type = data.get('type', 'message')

        if msg_type == 'message':
            text = data.get('text', '')
            message_id = hashlib.md5(f"{username}{datetime.now()}".encode()).hexdigest()[:8]
            
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

        elif msg_type == 'change_username':
            new_username = data.get('new_username', '')
            await self.change_username(ws, username, new_username)

        elif msg_type == 'private_message':
            to_username = data.get('to')
            text = data.get('text', '')
            if to_username:
                await self.send_private_message_parts(username, to_username, text)

        elif msg_type == 'call_offer':
            target = data.get('to')
            for client_ws, client_data in self.clients.items():
                if client_data['username'] == target:
                    await client_ws.send_str(json.dumps({
                        'type': 'call_offer',
                        'from': username,
                        'offer': data.get('offer')
                    }))
                    break

        elif msg_type == 'call_answer':
            target = data.get('to')
            for client_ws, client_data in self.clients.items():
                if client_data['username'] == target:
                    await client_ws.send_str(json.dumps({
                        'type': 'call_answer',
                        'from': username,
                        'answer': data.get('answer')
                    }))
                    break

        elif msg_type == 'call_reject':
            target = data.get('to')
            for client_ws, client_data in self.clients.items():
                if client_data['username'] == target:
                    await client_ws.send_str(json.dumps({
                        'type': 'system',
                        'message': f'📞 {username} отклонил звонок'
                    }))
                    break

        elif msg_type == 'call_end':
            target = data.get('to')
            for client_ws, client_data in self.clients.items():
                if client_data['username'] == target:
                    await client_ws.send_str(json.dumps({
                        'type': 'system',
                        'message': f'📞 {username} завершил звонок'
                    }))
                    break

        elif msg_type == 'typing':
            await self.broadcast({
                'type': 'typing',
                'username': username,
                'is_typing': data.get('is_typing', False)
            })
            
        elif msg_type == 'mark_spam':
            spammer = data.get('spammer')
            if spammer and spammer != username:
                if username not in self.spam_filters:
                    self.spam_filters[username] = []
                if spammer not in self.spam_filters[username]:
                    self.spam_filters[username].append(spammer)
                    spam_key = f"{spammer}"
                    self.spam_scores[spam_key] = self.spam_scores.get(spam_key, 0) + 1
                    await ws.send_str(json.dumps({
                        'type': 'system',
                        'message': f'✅ Пользователь {spammer} добавлен в черный список'
                    }))
                    await self.broadcast_spam_stats()
                    
        elif msg_type == 'unmark_spam':
            spammer = data.get('spammer')
            if spammer and username in self.spam_filters and spammer in self.spam_filters[username]:
                self.spam_filters[username].remove(spammer)
                await ws.send_str(json.dumps({
                    'type': 'system',
                    'message': f'✅ Пользователь {spammer} удален из черного списка'
                }))
                await self.broadcast_spam_stats()

        elif msg_type == 'get_spam_list':
            spam_list = self.spam_filters.get(username, [])
            await ws.send_str(json.dumps({
                'type': 'spam_list',
                'spammers': spam_list
            }))
            
        elif msg_type == 'ping':
            await ws.send_str(json.dumps({'type': 'pong'}))
            
    async def broadcast_spam_stats(self):
        spam_stats = {}
        for user, spammers in self.spam_filters.items():
            for spammer in spammers:
                spam_stats[spammer] = spam_stats.get(spammer, 0) + 1
        await self.broadcast({
            'type': 'spam_stats',
            'stats': spam_stats
        })

chat_processor = ChatServer()

HTML_PAGE = r'''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
    <title>Веб-чат с звонками</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0d1117;
            color: #f0f6fc;
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
        .input-area {
            background: #161b22;
            border-bottom: 1px solid #30363d;
            padding: 10px 12px;
            display: flex;
            gap: 8px;
            flex-shrink: 0;
        }
        .chat-header {
            background: #161b22;
            border-bottom: 1px solid #30363d;
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
        }
        .toggle-users-btn, .change-name-btn {
            background: #21262d;
            border: 1px solid #30363d;
            color: #f0f6fc;
            padding: 5px 10px;
            border-radius: 20px;
            cursor: pointer;
            font-size: 0.8em;
        }
        .users-sidebar {
            width: 280px;
            background: #161b22;
            border-right: 1px solid #30363d;
            display: none;
            flex-direction: column;
            overflow: hidden;
        }
        .users-sidebar.show {
            display: flex;
        }
        .users-header {
            padding: 10px;
            border-bottom: 1px solid #30363d;
            font-weight: bold;
            background: #21262d;
        }
        .search-box {
            padding: 8px;
            border-bottom: 1px solid #30363d;
        }
        .search-input {
            width: 100%;
            padding: 8px 12px;
            background: #0d1117;
            border: 1px solid #30363d;
            color: #f0f6fc;
            border-radius: 20px;
            outline: none;
        }
        .filter-buttons {
            padding: 8px;
            display: flex;
            gap: 8px;
            border-bottom: 1px solid #30363d;
        }
        .filter-btn {
            flex: 1;
            padding: 5px 8px;
            background: #21262d;
            border: 1px solid #30363d;
            color: #8b949e;
            border-radius: 15px;
            cursor: pointer;
            font-size: 0.75em;
        }
        .filter-btn.active {
            background: #58a6ff;
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
            flex-wrap: wrap;
        }
        .user-item:hover { background: #21262d; }
        .user-item.spam { background: #6e3a3a; opacity: 0.7; }
        .user-avatar {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #238636;
        }
        .user-avatar.spam { background: #da3633; }
        .user-name { flex: 1; font-size: 0.85em; }
        .call-btn, .reject-btn {
            border: none;
            border-radius: 15px;
            padding: 4px 10px;
            cursor: pointer;
            font-size: 0.7em;
            color: white;
        }
        .call-btn { background: #238636; }
        .call-btn.ongoing { background: #da3633; }
        .reject-btn { background: #da3633; margin-left: 5px; }
        .private-badge, .spam-badge {
            font-size: 0.7em;
            padding: 2px 6px;
            border-radius: 10px;
            cursor: pointer;
        }
        .private-badge { background: #3a4a6e; }
        .spam-badge { background: #da3633; }
        .messages-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        .chat-tabs {
            display: flex;
            gap: 2px;
            background: #161b22;
            border-bottom: 1px solid #30363d;
            padding: 5px 10px;
            overflow-x: auto;
            flex-shrink: 0;
        }
        .chat-tab {
            padding: 6px 12px;
            background: #21262d;
            border: none;
            color: #8b949e;
            cursor: pointer;
            border-radius: 6px;
            white-space: nowrap;
        }
        .chat-tab.active {
            background: #58a6ff;
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
        .message.system .message-bubble {
            background: #21262d;
            color: #8b949e;
            font-size: 0.75em;
            padding: 5px 12px;
            border-radius: 20px;
        }
        .message.own { justify-content: flex-end; }
        .message-bubble {
            max-width: 80%;
            padding: 8px 12px;
            border-radius: 18px;
        }
        .message:not(.own) .message-bubble {
            background: #21262d;
            border-bottom-left-radius: 4px;
        }
        .message.own .message-bubble {
            background: #58a6ff;
            border-bottom-right-radius: 4px;
        }
        .message-username {
            font-size: 0.7em;
            font-weight: bold;
            margin-bottom: 3px;
            color: #58a6ff;
        }
        .message-text {
            font-size: 0.85em;
            word-wrap: break-word;
            white-space: pre-wrap;
        }
        .message-time {
            font-size: 0.6em;
            opacity: 0.7;
            margin-top: 3px;
            text-align: right;
        }
        .typing-indicator {
            padding: 6px 16px;
            font-size: 0.75em;
            color: #8b949e;
            font-style: italic;
            min-height: 32px;
            background: #0d1117;
            flex-shrink: 0;
        }
        .message-input {
            flex: 1;
            background: #21262d;
            border: 1px solid #30363d;
            color: #f0f6fc;
            padding: 10px 14px;
            border-radius: 8px;
            font-family: inherit;
            outline: none;
            resize: none;
            max-height: 120px;
            min-height: 40px;
        }
        .send-btn {
            background: #58a6ff;
            color: white;
            border: none;
            padding: 0 20px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: bold;
        }
        .chat-title h1 { font-size: 1.1em; }
        .online-status {
            background: #238636;
            color: white;
            padding: 3px 8px;
            border-radius: 20px;
            font-size: 0.7em;
        }
        .username-display {
            background: #21262d;
            padding: 3px 8px;
            border-radius: 20px;
            font-size: 0.8em;
        }
        @media (max-width: 768px) {
            .users-sidebar {
                width: 100%;
                position: absolute;
                left: 0;
                right: 0;
                height: 100%;
                z-index: 1000;
            }
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
        var spamStats = {};
        var spamList = [];
        var activeCalls = new Map();
        var pendingCalls = new Map();
        
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
            var hours = d.getHours();
            var minutes = d.getMinutes();
            return (hours < 10 ? '0' + hours : hours) + ':' + (minutes < 10 ? '0' + minutes : minutes);
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
            var textWithBreaks = escapeHtml(message.text).split(/\n/).join('<br>');
            div.innerHTML = '<div class="message-bubble"><div class="message-username">' + escapeHtml(message.username) + '</div><div class="message-text">' + textWithBreaks + '</div><div class="message-time">' + formatTime(message.timestamp) + '</div></div>';
            messagesContainer.appendChild(div);
            scrollToBottom();
        }
        
        function addPrivateMessageToChat(message, otherUser) {
            var div = document.createElement('div');
            var isFromMe = (message.from === currentUser);
            div.className = 'message ' + (isFromMe ? 'own' : '');
            var sender = isFromMe ? 'Вы' : message.from;
            var textWithBreaks = escapeHtml(message.text).split(/\n/).join('<br>');
            div.innerHTML = '<div class="message-bubble"><div class="message-username">' + escapeHtml(sender) + '</div><div class="message-text">' + textWithBreaks + '</div><div class="message-time">' + formatTime(message.timestamp) + '</div></div>';
            messagesContainer.appendChild(div);
            scrollToBottom();
        }
        
        function showNotification(username) {
            var tabs = document.getElementById('chatTabs');
            var tab = null;
            for (var i = 0; i < tabs.children.length; i++) {
                if (tabs.children[i].getAttribute('data-chat') === username) {
                    tab = tabs.children[i];
                    break;
                }
            }
            if (tab && currentChat !== username) {
                tab.style.background = '#ff9800';
                setTimeout(function() {
                    if (currentChat !== username) tab.style.background = '';
                }, 1000);
            }
        }
        
        function addPrivateChatTab(username) {
            var tabsContainer = document.getElementById('chatTabs');
            var existing = null;
            for (var i = 0; i < tabsContainer.children.length; i++) {
                if (tabsContainer.children[i].getAttribute('data-chat') === username) {
                    existing = tabsContainer.children[i];
                    break;
                }
            }
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
            var tabToRemove = null;
            for (var i = 0; i < tabsContainer.children.length; i++) {
                if (tabsContainer.children[i].getAttribute('data-chat') === username) {
                    tabToRemove = tabsContainer.children[i];
                    break;
                }
            }
            if (tabToRemove) tabToRemove.remove();
            if (currentChat === username) window.switchChat('main');
        };
        
        window.switchChat = function(chatId) {
            currentChat = chatId;
            var tabs = document.querySelectorAll('.chat-tab');
            for (var i = 0; i < tabs.length; i++) {
                var tab = tabs[i];
                var tabChat = tab.getAttribute('data-chat');
                if ((chatId === 'main' && tabChat === 'main') || (chatId !== 'main' && tabChat === chatId)) {
                    tab.classList.add('active');
                } else {
                    tab.classList.remove('active');
                }
            }
            messagesContainer.innerHTML = '';
            if (chatId === 'main') {
                for (var i = 0; i < window.messagesHistory.length; i++) {
                    var msg = window.messagesHistory[i];
                    if (msg.type === 'message') addMessageToChat(msg);
                }
            } else {
                var messages = privateChats.get(chatId) || [];
                for (var i = 0; i < messages.length; i++) {
                    addPrivateMessageToChat(messages[i], chatId);
                }
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
        
        // ========== ЗВОНКИ ==========
        
        function updateCallButton(username, isCallActive) {
            var userItems = document.querySelectorAll('.user-item');
            for (var i = 0; i < userItems.length; i++) {
                var item = userItems[i];
                var nameElem = item.querySelector('.user-name');
                if (nameElem && nameElem.textContent.startsWith(username)) {
                    var oldCallBtn = item.querySelector('.call-btn');
                    if (oldCallBtn) oldCallBtn.remove();
                    var oldRejectBtn = item.querySelector('.reject-btn');
                    if (oldRejectBtn) oldRejectBtn.remove();
                    
                    if (isCallActive) {
                        var endBtn = document.createElement('button');
                        endBtn.textContent = '🔴 Положить';
                        endBtn.className = 'call-btn ongoing';
                        endBtn.onclick = (function(u) { return function(e) {
                            e.stopPropagation();
                            window.endCall(u);
                        }; })(username);
                        item.appendChild(endBtn);
                    } else {
                        var callBtn = document.createElement('button');
                        callBtn.textContent = '📞 Звонок';
                        callBtn.className = 'call-btn';
                        callBtn.onclick = (function(u) { return function(e) {
                            e.stopPropagation();
                            window.startCall(u);
                        }; })(username);
                        item.appendChild(callBtn);
                    }
                    break;
                }
            }
        }
        
        function rejectCall(fromUsername) {
            pendingCalls.delete(fromUsername);
            ws.send(JSON.stringify({ type: 'call_reject', to: fromUsername }));
            showSystemMessage('📞 Вы отклонили звонок от ' + fromUsername);
            
            var userItems = document.querySelectorAll('.user-item');
            for (var i = 0; i < userItems.length; i++) {
                var item = userItems[i];
                var nameElem = item.querySelector('.user-name');
                if (nameElem && nameElem.textContent.startsWith(fromUsername)) {
                    var oldCallBtn = item.querySelector('.call-btn');
                    if (oldCallBtn) oldCallBtn.remove();
                    var oldRejectBtn = item.querySelector('.reject-btn');
                    if (oldRejectBtn) oldRejectBtn.remove();
                    
                    var callBtn = document.createElement('button');
                    callBtn.textContent = '📞 Звонок';
                    callBtn.className = 'call-btn';
                    callBtn.onclick = (function(u) { return function(e) {
                        e.stopPropagation();
                        window.startCall(u);
                    }; })(fromUsername);
                    item.appendChild(callBtn);
                    break;
                }
            }
        }
        
        function showIncomingCall(fromUsername, offer) {
            pendingCalls.set(fromUsername, offer);
            showSystemMessage('📞 ' + fromUsername + ' звонит вам! Нажмите "Ответить" в списке пользователей');
            
            var userItems = document.querySelectorAll('.user-item');
            for (var i = 0; i < userItems.length; i++) {
                var item = userItems[i];
                var nameElem = item.querySelector('.user-name');
                if (nameElem && nameElem.textContent.startsWith(fromUsername)) {
                    var oldCallBtn = item.querySelector('.call-btn');
                    if (oldCallBtn) oldCallBtn.remove();
                    var oldRejectBtn = item.querySelector('.reject-btn');
                    if (oldRejectBtn) oldRejectBtn.remove();
                    
                    var answerBtn = document.createElement('button');
                    answerBtn.textContent = '✅ Ответить';
                    answerBtn.className = 'call-btn';
                    answerBtn.style.background = '#238636';
                    answerBtn.onclick = (function(u, o) { return function(e) {
                        e.stopPropagation();
                        answerCall(u, o);
                    }; })(fromUsername, offer);
                    item.appendChild(answerBtn);
                    
                    var rejectBtn = document.createElement('button');
                    rejectBtn.textContent = '❌ Отклонить';
                    rejectBtn.className = 'reject-btn';
                    rejectBtn.onclick = (function(u) { return function(e) {
                        e.stopPropagation();
                        rejectCall(u);
                    }; })(fromUsername);
                    item.appendChild(rejectBtn);
                    break;
                }
            }
        }
        
        async function answerCall(fromUsername, offer) {
            var userItems = document.querySelectorAll('.user-item');
            for (var i = 0; i < userItems.length; i++) {
                var item = userItems[i];
                var nameElem = item.querySelector('.user-name');
                if (nameElem && nameElem.textContent.startsWith(fromUsername)) {
                    var btn = item.querySelector('.call-btn');
                    if (btn) btn.remove();
                    var rbtn = item.querySelector('.reject-btn');
                    if (rbtn) rbtn.remove();
                    break;
                }
            }
            
            try {
                showSystemMessage('📞 Отвечаем ' + fromUsername + '...');
                var stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                var pc = new RTCPeerConnection({
                    iceServers: [
                        { urls: 'stun:stun.l.google.com:19302' },
                        { urls: 'stun:stun1.l.google.com:19302' }
                    ]
                });
                
                stream.getTracks().forEach(function(track) { pc.addTrack(track, stream); });
                activeCalls.set(fromUsername, { pc: pc, stream: stream });
                
                pc.ontrack = function(event) {
                    var audio = new Audio();
                    audio.srcObject = event.streams[0];
                    audio.autoplay = true;
                    showSystemMessage('📞 Разговор с ' + fromUsername + ' начался');
                    updateCallButton(fromUsername, true);
                };
                
                pc.oniceconnectionstatechange = function() {
                    if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed') {
                        window.endCall(fromUsername);
                    }
                };
                
                await pc.setRemoteDescription(new RTCSessionDescription(offer));
                var answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                
                ws.send(JSON.stringify({
                    type: 'call_answer',
                    to: fromUsername,
                    answer: { sdp: answer.sdp, type: answer.type }
                }));
                
                updateCallButton(fromUsername, true);
                pendingCalls.delete(fromUsername);
                
            } catch (error) {
                console.error('Ошибка ответа:', error);
                showSystemMessage('❌ Не удалось ответить на звонок. Проверьте микрофон.');
                updateCallButton(fromUsername, false);
                pendingCalls.delete(fromUsername);
            }
        }
        
        window.startCall = async function(targetUsername) {
            if (targetUsername === currentUser) {
                showSystemMessage('Нельзя позвонить самому себе');
                return;
            }
            if (activeCalls.has(targetUsername)) {
                showSystemMessage('У вас уже есть активный звонок с этим пользователем');
                return;
            }
            if (pendingCalls.has(targetUsername)) {
                showSystemMessage('Пользователь уже звонит вам');
                return;
            }
            
            try {
                showSystemMessage('📞 Запрашиваем доступ к микрофону...');
                var stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                var pc = new RTCPeerConnection({
                    iceServers: [
                        { urls: 'stun:stun.l.google.com:19302' },
                        { urls: 'stun:stun1.l.google.com:19302' }
                    ]
                });
                
                stream.getTracks().forEach(function(track) { pc.addTrack(track, stream); });
                activeCalls.set(targetUsername, { pc: pc, stream: stream });
                
                pc.ontrack = function(event) {
                    var audio = new Audio();
                    audio.srcObject = event.streams[0];
                    audio.autoplay = true;
                    showSystemMessage('📞 Разговор с ' + targetUsername + ' начался');
                    updateCallButton(targetUsername, true);
                };
                
                pc.oniceconnectionstatechange = function() {
                    if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed') {
                        window.endCall(targetUsername);
                    }
                };
                
                var offer = await pc.createOffer();
                await pc.setLocalDescription(offer);
                
                ws.send(JSON.stringify({
                    type: 'call_offer',
                    to: targetUsername,
                    offer: { sdp: offer.sdp, type: offer.type }
                }));
                
                showSystemMessage('📞 Звоним ' + targetUsername + '...');
                updateCallButton(targetUsername, true);
                
            } catch (error) {
                console.error('Ошибка звонка:', error);
                showSystemMessage('❌ Не удалось начать звонок (нет доступа к микрофону)');
                if (activeCalls.has(targetUsername)) {
                    activeCalls.delete(targetUsername);
                }
                updateCallButton(targetUsername, false);
            }
        };
        
        window.endCall = function(targetUsername) {
            var call = activeCalls.get(targetUsername);
            if (call) {
                if (call.stream) {
                    call.stream.getTracks().forEach(function(track) { track.stop(); });
                }
                if (call.pc) {
                    call.pc.close();
                }
                activeCalls.delete(targetUsername);
            }
            ws.send(JSON.stringify({ type: 'call_end', to: targetUsername }));
            showSystemMessage('📞 Звонок с ' + targetUsername + ' завершен');
            updateCallButton(targetUsername, false);
            pendingCalls.delete(targetUsername);
        };
        
        // ========== ОСТАЛЬНЫЕ ФУНКЦИИ ==========
        
        function isSpamUser(username) {
            for (var i = 0; i < spamList.length; i++) {
                if (spamList[i] === username) return true;
            }
            return false;
        }
        
        function filterUsers() {
            var searchValue = userSearch.value.toLowerCase();
            var filtered = [];
            for (var i = 0; i < allUsers.length; i++) {
                var user = allUsers[i];
                if (searchValue && user.name.toLowerCase().indexOf(searchValue) === -1) continue;
                if (currentUserFilter === 'spam' && !isSpamUser(user.name)) continue;
                if (currentUserFilter === 'clean' && isSpamUser(user.name)) continue;
                filtered.push(user);
            }
            filtered.sort(function(a, b) {
                if (a.name === currentUser) return -1;
                if (b.name === currentUser) return 1;
                if (a.name < b.name) return -1;
                if (a.name > b.name) return 1;
                return 0;
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
                
                html += '<div class="user-item ' + (isSpam ? 'spam' : '') + '" onclick="' + (isCurrent ? '' : 'startPrivateChat(\'' + escapeHtml(user.name) + '\')') + '">';
                html += '<div class="user-avatar ' + (isSpam ? 'spam' : '') + '"></div>';
                html += '<div class="user-name">' + escapeHtml(user.name) + (isCurrent ? ' (Вы)' : '') + sessionsHtml + statsHtml + '</div>';
                
                if (!isCurrent) {
                    html += '<button class="call-btn" onclick="event.stopPropagation(); startCall(\'' + escapeHtml(user.name) + '\')">📞 Звонок</button>';
                    var badgeText = isSpam ? 'Снять спам' : 'Спам';
                    var badgeClass = isSpam ? 'spam-badge' : 'private-badge';
                    html += '<span class="' + badgeClass + '" onclick="event.stopPropagation(); toggleSpam(\'' + escapeHtml(user.name) + '\')">' + badgeText + '</span>';
                }
                html += '</div>';
            }
            usersList.innerHTML = html;
            
            for (var [username, call] of activeCalls) {
                if (call && call.pc) {
                    updateCallButton(username, true);
                }
            }
        }
        
        window.toggleSpam = function(username) {
            if (isSpamUser(username)) {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'unmark_spam', spammer: username }));
                }
                var newList = [];
                for (var i = 0; i < spamList.length; i++) {
                    if (spamList[i] !== username) newList.push(spamList[i]);
                }
                spamList = newList;
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
            for (var i = 0; i < btns.length; i++) {
                var btn = btns[i];
                btn.classList.remove('active');
                if (btn.getAttribute('data-filter') === filter) btn.classList.add('active');
            }
            filterUsers();
        };
        
        window.toggleUsers = function() {
            if (usersSidebar.classList.contains('show')) {
                usersSidebar.classList.remove('show');
            } else {
                usersSidebar.classList.add('show');
                filterUsers();
            }
        };
        
        window.changeUsername = function() {
            var newName = prompt('Введите новое имя (макс. 20 символов):', currentUser);
            if (newName && newName.trim() && newName.trim() !== currentUser) {
                var trimmedName = newName.trim().substring(0, 20);
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ 
                        type: 'change_username', 
                        new_username: trimmedName 
                    }));
                } else {
                    showSystemMessage('❌ Нет соединения с сервером');
                }
            }
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
                var names = [];
                typingUsers.forEach(function(name) { names.push(name); });
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
            var isSpam = false;
            for (var i = 0; i < spamList.length; i++) {
                if (spamList[i] === message.from) isSpam = true;
            }
            if (!isFromMe && isSpam) return;
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
                    if (data.message && data.message.indexOf('Вы успешно сменили имя') !== -1) {
                        var match = data.message.match(/на (.+)$/);
                        if (match && match[1]) {
                            currentUser = match[1];
                            currentUsernameSpan.textContent = currentUser;
                            localStorage.setItem('chat_username', currentUser);
                        }
                    }
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
                case 'call_offer':
                    showIncomingCall(data.from, data.offer);
                    break;
                case 'call_answer':
                    var call = activeCalls.get(data.from);
                    if (call && call.pc) {
                        call.pc.setRemoteDescription(new RTCSessionDescription(data.answer));
                    }
                    break;
                case 'call_reject':
                    showSystemMessage('📞 ' + data.message);
                    if (pendingCalls.has(data.from)) pendingCalls.delete(data.from);
                    updateCallButton(data.from, false);
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
        
        var filterBtns = document.querySelectorAll('.filter-btn');
        for (var i = 0; i < filterBtns.length; i++) {
            filterBtns[i].onclick = function() {
                window.setUserFilter(this.getAttribute('data-filter'));
            };
        }
        
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

app = web.Application()
app.router.add_get('/', handle_index)
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/healthz', health_check)

if __name__ == "__main__":
    print(f"Server starting on port {PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)
