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

# --- Встроенный HTML (полностью исправленная версия) ---
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
            font-size: 0.85em;
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
            font-size: 0.85em;
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
            transition: all 0.2s;
        }
        
        .filter-btn.active {
            background: var(--accent);
            color: white;
            border-color: var(--accent);
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
            font-size: 0.85em;
            cursor: pointer;
            transition: background 0.2s;
            position: relative;
        }
        
        .user-item:hover { background: var(--bg-tertiary); }
        .user-item:active { background: var(--bg-tertiary); }
        .user-item.spam { 
            background: var(--spam);
            opacity: 0.7;
        }
        .user-avatar { 
            width: 8px; 
            height: 8px; 
            border-radius: 50%; 
            background: var(--success); 
            flex-shrink: 0; 
        }
        .user-avatar.spam { background: var(--danger); }
        .user-name { 
            word-break: break-word; 
            flex: 1; 
            font-size: 0.85em;
        }
        .user-sessions { 
            font-size: 0.7em; 
            color: var(--text-secondary); 
            margin-left: 4px; 
        }
        .private-badge { 
            font-size: 0.7em; 
            background: var(--private-chat); 
            padding: 2px 6px; 
            border-radius: 10px; 
            margin-left: 5px;
            cursor: pointer;
        }
        .spam-badge {
            font-size: 0.7em;
            background: var(--danger);
            padding: 2px 6px;
            border-radius: 10px;
            margin-left: 5px;
            cursor: pointer;
        }
        .spam-badge:hover { opacity: 0.8; }
        .spam-stats {
            font-size: 0.65em;
            color: var(--warning);
            margin-left: 5px;
        }
        
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
            font-size: 0.85em;
            white-space: nowrap;
            transition: all 0.2s;
        }
        
        .chat-tab.active {
            background: var(--accent);
            color: white;
        }
        
        .chat-tab.private {
            background: var(--private-chat);
        }
        
        .close-tab {
            margin-left: 8px;
            cursor: pointer;
            font-weight: bold;
            opacity: 0.7;
        }
        
        .close-tab:hover {
            opacity: 1;
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
        .message.private { background: rgba(58, 74, 110, 0.2); }
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
        
        .char-counter.warning {
            color: orange;
        }
        
        .char-counter.danger {
            color: red;
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
            .users-sidebar { width: 100%; position: absolute; left: 0; right: 0; height: 100%; z-index: 1000; }
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
            <button class="send-btn" id="sendButton">📨 Отправить</button>
        </div>
        
        <div class="chat-header">
            <div class="chat-title">
                <h1>💬 Веб-чат</h1>
                <span class="online-status" id="onlineCount">0 онлайн</span>
            </div>
            <div class="user-info">
                <span class="username-display" id="currentUsername">Загрузка...</span>
                <button class="change-name-btn" id="changeNameBtn">Сменить имя</button>
                <button class="toggle-users-btn" id="toggleUsersBtn">👥</button>
            </div>
        </div>
        
        <div class="chat-main" id="chatMain">
            <div class="users-sidebar" id="usersSidebar">
                <div class="users-header">👥 Участники (<span id="usersCount">0</span>)</div>
                <div class="search-box">
                    <input type="text" id="userSearch" class="search-input" placeholder="🔍 Поиск участников...">
                </div>
                <div class="filter-buttons">
                    <button class="filter-btn active" data-filter="all">Все</button>
                    <button class="filter-btn" data-filter="spam">🚫 Спам</button>
                    <button class="filter-btn" data-filter="clean">✅ Чистые</button>
                </div>
                <div class="users-list" id="usersList"><div>Подключение...</div></div>
            </div>
            <div class="messages-area">
                <div class="chat-tabs" id="chatTabs">
                    <button class="chat-tab active" data-chat="main">💬 Общий чат</button>
                </div>
                <div class="messages-container" id="messagesContainer"></div>
                <div class="char-counter" id="charCounter">0/1600</div>
                <div class="typing-indicator" id="typingIndicator"></div>
            </div>
        </div>
    </div>
    <script>
        // Глобальные переменные
        let ws = null;
        let currentUser = null;
        let sessionId = null;
        let typingTimeout = null;
        let isTyping = false;
        let typingUsers = new Set();
        let currentChat = 'main';
        let privateChats = new Map();
        let allUsers = [];
        let currentUserFilter = 'all';
        let searchQuery = '';
        let spamStats = {};
        let spamList = [];
        
        // DOM элементы
        const messagesContainer = document.getElementById('messagesContainer');
        const messageInput = document.getElementById('messageInput');
        const typingIndicator = document.getElementById('typingIndicator');
        const currentUsernameSpan = document.getElementById('currentUsername');
        const onlineCountSpan = document.getElementById('onlineCount');
        const usersCountSpan = document.getElementById('usersCount');
        const usersList = document.getElementById('usersList');
        const usersSidebar = document.getElementById('usersSidebar');
        const charCounter = document.getElementById('charCounter');
        const sendButton = document.getElementById('sendButton');
        const changeNameBtn = document.getElementById('changeNameBtn');
        const toggleUsersBtn = document.getElementById('toggleUsersBtn');
        const userSearch = document.getElementById('userSearch');
        
        // История сообщений
        window.messagesHistory = [];
        
        // Функции
        function getSessionId() {
            let id = localStorage.getItem('chat_session_id');
            if (!id) {
                id = 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                localStorage.setItem('chat_session_id', id);
            }
            return id;
        }
        
        window.toggleUsers = function() {
            usersSidebar.classList.toggle('show');
            if (usersSidebar.classList.contains('show')) {
                filterUsers();
            }
        };
        
        window.setUserFilter = function(filter) {
            currentUserFilter = filter;
            document.querySelectorAll('.filter-btn').forEach(btn => {
                btn.classList.remove('active');
                if (btn.getAttribute('data-filter') === filter) {
                    btn.classList.add('active');
                }
            });
            filterUsers();
        };
        
        function filterUsers() {
            searchQuery = userSearch.value.toLowerCase();
            
            let filteredUsers = [...allUsers];
            
            if (searchQuery) {
                filteredUsers = filteredUsers.filter(user => 
                    user.name.toLowerCase().includes(searchQuery)
                );
            }
            
            if (currentUserFilter === 'spam') {
                filteredUsers = filteredUsers.filter(user => isSpamUser(user.name));
            } else if (currentUserFilter === 'clean') {
                filteredUsers = filteredUsers.filter(user => !isSpamUser(user.name));
            }
            
            filteredUsers.sort((a, b) => a.name.localeCompare(b.name));
            
            if (filteredUsers.length === 0) {
                usersList.innerHTML = '<div style="padding: 10px; text-align: center; color: var(--text-secondary);">👤 Пользователи не найдены</div>';
                return;
            }
            
            usersList.innerHTML = filteredUsers.map(user => {
                let sessionsHtml = '';
                if (user.sessions > 1) {
                    sessionsHtml = '<span class="user-sessions">📱 ' + user.sessions + ' вкладки</span>';
                }
                const isCurrent = user.name === currentUser;
                const isSpamUserFlag = isSpamUser(user.name);
                const spamCount = spamStats[user.name] || 0;
                const spamStatsHtml = spamCount > 0 ? '<span class="spam-stats">⚠️ ' + spamCount + '</span>' : '';
                
                const onClick = isCurrent ? '' : 'onclick="startPrivateChat(\'' + escapeHtml(user.name) + '\')"';
                const onSpamToggle = !isCurrent ? 'onclick="event.stopPropagation(); toggleSpam(\'' + escapeHtml(user.name) + '\')"' : '';
                
                let result = '<div class="user-item ' + (isSpamUserFlag ? 'spam' : '') + '" ' + onClick + '>';
                result += '<div class="user-avatar ' + (isSpamUserFlag ? 'spam' : '') + '"></div>';
                result += '<div class="user-name">' + escapeHtml(user.name) + (isCurrent ? ' (Вы)' : '') + sessionsHtml + '</div>';
                result += spamStatsHtml;
                if (!isCurrent) {
                    result += '<span class="' + (isSpamUserFlag ? 'spam-badge' : 'private-badge') + '" ' + onSpamToggle + '>' + (isSpamUserFlag ? '🚫 Снять спам' : '⚠️ Спам') + '</span>';
                }
                if (!isCurrent && !isSpamUserFlag) {
                    result += '<span class="private-badge" onclick="event.stopPropagation(); startPrivateChat(\'' + escapeHtml(user.name) + '\')">💬</span>';
                }
                result += '</div>';
                return result;
            }).join('');
        }
        
        function isSpamUser(username) {
            return spamList.includes(username);
        }
        
        window.toggleSpam = function(username) {
            if (isSpamUser(username)) {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ 
                        type: 'unmark_spam', 
                        spammer: username 
                    }));
                }
                const index = spamList.indexOf(username);
                if (index > -1) spamList.splice(index, 1);
                showSystemMessage('✅ ' + username + ' удален из черного списка');
            } else {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ 
                        type: 'mark_spam', 
                        spammer: username 
                    }));
                }
                spamList.push(username);
                showSystemMessage('⚠️ ' + username + ' отмечен как спам. Сообщения от него не будут приходить.');
            }
            filterUsers();
        };
        
        function updateSpamList() {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'get_spam_list' }));
            }
        }
        
        function connect(username, sessionId) {
            const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = wsProtocol + '//' + window.location.host + '/ws';
            ws = new WebSocket(wsUrl);
            
            ws.onopen = () => {
                console.log('Connected');
                ws.send(JSON.stringify({ username: username, session_id: sessionId }));
                setInterval(() => {
                    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' }));
                }, 30000);
                setTimeout(() => updateSpamList(), 1000);
            };
            ws.onmessage = (event) => { 
                const data = JSON.parse(event.data); 
                handleMessage(data); 
            };
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
                    showSystemMessage(data.message); 
                    if (data.users_count) updateOnlineCount(data.users_count); 
                    break;
                case 'users_list': 
                    allUsers = data.users;
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
        
        function handlePrivateMessage(message) {
            const isFromMe = message.from === currentUser;
            const otherUser = isFromMe ? message.to : message.from;
            
            if (!isFromMe && spamList.includes(message.from)) {
                return;
            }
            
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
        
        function addPrivateChatTab(username) {
            const tabsContainer = document.getElementById('chatTabs');
            const existingTab = Array.from(tabsContainer.children).find(
                tab => tab.textContent.includes(username)
            );
            if (existingTab) return;
            
            const tab = document.createElement('button');
            tab.className = 'chat-tab private';
            tab.setAttribute('data-chat', username);
            tab.innerHTML = '💬 ' + username + ' <span class="close-tab" onclick="event.stopPropagation(); window.closePrivateChat(\'' + username + '\')">✖</span>';
            tab.onclick = function() { window.switchChat(username); };
            tabsContainer.appendChild(tab);
        }
        
        window.closePrivateChat = function(username) {
            privateChats.delete(username);
            const tabsContainer = document.getElementById('chatTabs');
            const tab = Array.from(tabsContainer.children).find(
                t => t.getAttribute('data-chat') === username
            );
            if (tab) tab.remove();
            
            if (currentChat === username) {
                window.switchChat('main');
            }
        };
        
        window.switchChat = function(chatId) {
            currentChat = chatId;
            
            const tabs = document.querySelectorAll('.chat-tab');
            tabs.forEach(tab => {
                const tabChat = tab.getAttribute('data-chat');
                if ((chatId === 'main' && tabChat === 'main') ||
                    (chatId !== 'main' && tabChat === chatId)) {
                    tab.classList.add('active');
                } else {
                    tab.classList.remove('active');
                }
            });
            
            messagesContainer.innerHTML = '';
            
            if (chatId === 'main') {
                window.messagesHistory.forEach(msg => {
                    if (msg.type === 'message') {
                        addMessageToChat(msg);
                    }
                });
            } else {
                const messages = privateChats.get(chatId) || [];
                messages.forEach(msg => {
                    addPrivateMessageToChat(msg, chatId);
                });
            }
            scrollToBottom();
        };
        
        function addPrivateMessageToChat(message, otherUser) {
            const messageDiv = document.createElement('div');
            const isFromMe = message.from === currentUser;
            messageDiv.className = 'message ' + (isFromMe ? 'own' : '') + ' private';
            const sender = isFromMe ? 'Вы' : message.from;
            const textWithBreaks = escapeHtml(message.text).replace(/\\\\n/g, '<br>');
            messageDiv.innerHTML = '<div class="message-bubble"><div class="message-username">' + escapeHtml(sender) + '</div><div class="message-text">' + textWithBreaks + '</div><div class="message-time">' + formatTime(message.timestamp) + '</div></div>';
            messagesContainer.appendChild(messageDiv);
            scrollToBottom();
        }
        
        function addMessageToChat(message) {
            const messageDiv = document.createElement('div');
            messageDiv.className = 'message ' + (message.username === currentUser ? 'own' : '');
            const textWithBreaks = escapeHtml(message.text).replace(/\\\\n/g, '<br>');
            messageDiv.innerHTML = '<div class="message-bubble"><div class="message-username">' + escapeHtml(message.username) + '</div><div class="message-text">' + textWithBreaks + '</div><div class="message-time">' + formatTime(message.timestamp) + '</div></div>';
            messagesContainer.appendChild(messageDiv);
            scrollToBottom();
        }
        
        function showSystemMessage(text) { 
            if (currentChat !== 'main') return;
            const messageDiv = document.createElement('div'); 
            messageDiv.className = 'message system'; 
            messageDiv.innerHTML = '<div class="message-bubble">' + escapeHtml(text) + '</div>'; 
            messagesContainer.appendChild(messageDiv); 
            scrollToBottom(); 
        }
        
        function showNotification(username) {
            const tabsContainer = document.getElementById('chatTabs');
            const tab = Array.from(tabsContainer.children).find(
                t => t.getAttribute('data-chat') === username
            );
            if (tab && currentChat !== username) {
                tab.style.background = '#ff9800';
                setTimeout(() => {
                    if (currentChat !== username) {
                        tab.style.background = '';
                    }
                }, 1000);
            }
        }
        
        window.sendMessage = function() { 
            const text = messageInput.value;
            if (!text.trim() || !ws || ws.readyState !== WebSocket.OPEN) return;
            
            if (currentChat === 'main') {
                ws.send(JSON.stringify({ type: 'message', text: text }));
            } else {
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
        
        function handleKeyDown(event) {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                window.sendMessage();
            }
        }
        
        function updateCharCounter() {
            const length = messageInput.value.length;
            charCounter.textContent = length + '/1600';
            if (length > 1400) {
                charCounter.className = 'char-counter warning';
            } else if (length > 1600) {
                charCounter.className = 'char-counter danger';
            } else {
                charCounter.className = 'char-counter';
            }
        }
        
        function handleKeyUp(event) {
            updateCharCounter();
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
            onlineCountSpan.textContent = count + ' онлайн'; 
            filterUsers();
        }
        
        function updateOnlineCount(count) { 
            onlineCountSpan.textContent = count + ' онлайн'; 
            usersCountSpan.textContent = count; 
        }
        
        function updateTypingIndicator(username, isTypingUser) { 
            if (currentChat !== 'main') return;
            if (isTypingUser && username !== currentUser) typingUsers.add(username); 
            else typingUsers.delete(username); 
            if (typingUsers.size > 0) { 
                const names = Array.from(typingUsers); 
                let text = names.length === 1 ? names[0] + ' печатает...' : names.length === 2 ? names[0] + ' и ' + names[1] + ' печатают...' : names.length + ' человек печатают...'; 
                typingIndicator.textContent = text; 
            } else typingIndicator.textContent = ''; 
        }
        
        window.changeUsername = function() { 
            const newName = prompt('Введите новое имя (макс. 20 символов):', currentUser); 
            if (newName && newName.trim() && newName.trim() !== currentUser) { 
                currentUser = newName.trim().substring(0, 20); 
                currentUsernameSpan.textContent = currentUser; 
                localStorage.setItem('chat_username', currentUser);
                if (ws) ws.close(); 
                setTimeout(() => connect(currentUser, sessionId), 100); 
            } 
        };
        
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
            currentUser = prompt('Ваше имя:', 'Гость') || 'Гость_' + Math.floor(Math.random() * 1000); 
            localStorage.setItem('chat_username', currentUser); 
        }
        currentUsernameSpan.textContent = currentUser;
        
        // Сохраняем историю сообщений
        const originalAddMessage = addMessageToChat;
        window.addMessageToChat = function(message) {
            window.messagesHistory.push(message);
            if (window.messagesHistory.length > 100) window.messagesHistory.shift();
            originalAddMessage(message);
        };
        
        connect(currentUser, sessionId);
        
        // Назначение обработчиков событий
        messageInput.addEventListener('input', function(e) {
            autoResizeTextarea.call(messageInput);
            updateCharCounter();
        });
        messageInput.addEventListener('keydown', handleKeyDown);
        messageInput.addEventListener('keyup', handleKeyUp);
        sendButton.addEventListener('click', window.sendMessage);
        changeNameBtn.addEventListener('click', window.changeUsername);
        toggleUsersBtn.addEventListener('click', window.toggleUsers);
        userSearch.addEventListener('keyup', filterUsers);
        
        // Обработчики для кнопок фильтрации
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                window.setUserFilter(this.getAttribute('data-filter'));
            });
        });
        
        // Обработчик для вкладки общего чата
        const mainTab = document.querySelector('.chat-tab[data-chat="main"]');
        if (mainTab) {
            mainTab.addEventListener('click', function() {
                window.switchChat('main');
            });
        }
        
        messageInput.focus();
        
        document.addEventListener('click', function(event) {
            if (usersSidebar.classList.contains('show')) {
                if (!usersSidebar.contains(event.target) && !toggleUsersBtn.contains(event.target)) {
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
