import asyncio
import json
import os
import hashlib
import secrets
from datetime import datetime
from pathlib import Path
import subprocess
from aiohttp import web

# === НАСТРОЙКИ ===
PORT = int(os.environ.get("PORT", 8080))
MAX_MESSAGE_LENGTH = 1600
USERS_FILE = "users.json"
# =================

messages_history = []
MAX_HISTORY = 100
connected_clients = set()

class UserAuth:
    """Класс для работы с аутентификацией пользователей"""
    def __init__(self, users_file_path="users.json"):
        self.users_file_path = users_file_path
        self.users_cache = {}
        
    def load_users(self, force=False):
        """Загружает пользователей из JSON файла"""
        if not os.path.exists(self.users_file_path):
            print(f"⚠️ Файл {self.users_file_path} не найден, создаю пустой")
            self.users_cache = {}
            self._save_users()
            return True
            
        try:
            with open(self.users_file_path, 'r', encoding='utf-8') as f:
                self.users_cache = json.load(f)
            print(f"✅ Загружено {len(self.users_cache)} пользователей")
            return True
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка в JSON файле: {e}")
            return False
        except Exception as e:
            print(f"❌ Ошибка загрузки: {e}")
            return False
    
    def _save_users(self):
        """Сохраняет пользователей в JSON файл"""
        try:
            with open(self.users_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.users_cache, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"❌ Ошибка сохранения: {e}")
            return False
    
    def verify_password(self, email, password):
        """Проверяет пароль и статус пользователя"""
        self.load_users()
        
        if email not in self.users_cache:
            return False, "Пользователь не найден"
        
        user_data = self.users_cache[email]
        
        # Проверяем статус
        if user_data.get('status') == 'blocked':
            reason = user_data.get('blocked_reason', 'Не указана')
            return False, f"Аккаунт заблокирован. Причина: {reason}"
        
        # Проверяем пароль (новый формат с солью)
        if 'salt' in user_data and 'hash' in user_data:
            salt = bytes.fromhex(user_data['salt'])
            stored_hash = user_data['hash']
            
            test_hash = hashlib.pbkdf2_hmac(
                'sha256',
                password.encode('utf-8'),
                salt,
                100000
            ).hex()
            
            if test_hash == stored_hash:
                return True, None
            else:
                return False, "Неверный пароль"
        # Старый формат (для обратной совместимости)
        elif 'password_hash' in user_data:
            if user_data['password_hash'] == hashlib.sha256(password.encode()).hexdigest():
                return True, None
            else:
                return False, "Неверный пароль"
        
        return False, "Ошибка аутентификации"
    
    def get_user_name(self, email):
        """Возвращает имя пользователя по email (только из JSON, нельзя изменить)"""
        if email in self.users_cache:
            return self.users_cache[email].get('name', email.split('@')[0])
        return email.split('@')[0]
    
    def is_user_blocked(self, email):
        """Проверяет, заблокирован ли пользователь"""
        if email in self.users_cache:
            return self.users_cache[email].get('status') == 'blocked'
        return False
    
    def sync_from_github(self):
        """Подтягивает изменения из GitHub"""
        try:
            if os.path.exists('.git'):
                result = subprocess.run(
                    ['git', 'pull'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    print("🔄 Синхронизация с GitHub выполнена")
                    self.load_users(force=True)
                    return True
            return False
        except Exception as e:
            print(f"❌ Ошибка синхронизации: {e}")
            return False

# Инициализируем аутентификацию
user_auth = UserAuth(USERS_FILE)
user_auth.load_users()

class ChatServer:
    def __init__(self):
        self.clients = {}
        self.user_sessions = {}
        self.private_chats = {}
        self.spam_filters = {}
        self.spam_scores = {}

    def is_nickname_taken(self, username, exclude_ws=None):
        """Проверяет, есть ли уже пользователь с таким ником"""
        for ws, client_data in self.clients.items():
            if exclude_ws and ws == exclude_ws:
                continue
            if client_data['username'] == username:
                return True
        return False

    def generate_unique_nickname(self, base_nickname):
        """Если ник занят, добавляет число в конец"""
        if not self.is_nickname_taken(base_nickname):
            return base_nickname
        
        counter = 1
        while self.is_nickname_taken(f"{base_nickname}{counter}"):
            counter += 1
        return f"{base_nickname}{counter}"

    async def register(self, ws, email, username, session_id):
        """Регистрация подключения - имя ТОЛЬКО из JSON, нельзя изменить"""
        # Имя берётся строго из JSON, игнорируем то, что прислал клиент
        fixed_username = user_auth.get_user_name(email)
        
        # Проверка уникальности ника (на случай, если в JSON одинаковые имена)
        if self.is_nickname_taken(fixed_username):
            fixed_username = self.generate_unique_nickname(fixed_username)
            await ws.send_str(json.dumps({
                'type': 'system',
                'message': f'⚠️ Имя "{fixed_username}" уже занято. Вы вошли как "{fixed_username}"'
            }))
        
        self.clients[ws] = {
            'username': fixed_username, 
            'email': email,
            'session_id': session_id
        }
        connected_clients.add(ws)
        
        if fixed_username not in self.spam_filters:
            self.spam_filters[fixed_username] = []
        
        session_key = f"{fixed_username}_{session_id}"
        self.user_sessions[session_key] = self.user_sessions.get(session_key, 0) + 1

        # Отправляем историю сообщений
        for msg in messages_history[-50:]:
            try:
                await ws.send_str(json.dumps(msg))
            except:
                pass

        # Оповещаем о входе
        if self.user_sessions[session_key] == 1:
            await self.broadcast({
                'type': 'system',
                'message': f'👋 {fixed_username} присоединился к чату',
                'users_count': len(self.get_unique_users())
            })
        
        await self.broadcast_users_list()
        
        # Отправляем приветственное сообщение
        await ws.send_str(json.dumps({
            'type': 'system',
            'message': f'✅ Добро пожаловать в чат, {fixed_username}! Ваше имя зафиксировано в системе.'
        }))

    async def unregister(self, ws):
        """Отключение пользователя"""
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
        """Возвращает список уникальных пользователей"""
        user_sessions_count = {}
        for client_data in self.clients.values():
            username = client_data['username']
            user_sessions_count[username] = user_sessions_count.get(username, 0) + 1
        
        return [{'name': name, 'sessions': count} for name, count in user_sessions_count.items()]

    async def broadcast(self, message, exclude_ws=None):
        """Отправка сообщения всем"""
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
        """Отправка списка пользователей"""
        users_list = self.get_unique_users()
        await self.broadcast({
            'type': 'users_list',
            'users': users_list,
            'count': len(users_list)
        })

    async def send_private_message(self, from_username, to_username, text, message_id):
        """Отправка личного сообщения"""
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
        """Отправка длинного личного сообщения частями"""
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

    async def handle_message(self, ws, data):
        """Обработка сообщений от клиента"""
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
        """Отправка статистики спама"""
        spam_stats = {}
        for user, spammers in self.spam_filters.items():
            for spammer in spammers:
                spam_stats[spammer] = spam_stats.get(spammer, 0) + 1
        
        await self.broadcast({
            'type': 'spam_stats',
            'stats': spam_stats
        })

chat_processor = ChatServer()

# HTML страница
HTML_PAGE = r'''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no, viewport-fit=cover">
    <title>Защищённый чат</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            -webkit-tap-highlight-color: transparent;
        }

        :root {
            --window-height: 100vh;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0d1117;
            color: #f0f6fc;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            overflow: hidden;
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
        }

        /* Экран авторизации */
        .auth-screen {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 2000;
            transition: opacity 0.3s ease;
        }

        .auth-screen.hidden {
            opacity: 0;
            pointer-events: none;
        }

        .auth-container {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            padding: 30px 25px;
            width: 90%;
            max-width: 400px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }

        .auth-container h2 {
            color: #333;
            text-align: center;
            margin-bottom: 25px;
            font-size: 24px;
        }

        .auth-input {
            width: 100%;
            padding: 12px 15px;
            margin: 10px 0;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            font-size: 16px;
            transition: border-color 0.3s;
        }

        .auth-input:focus {
            outline: none;
            border-color: #667eea;
        }

        .auth-button {
            width: 100%;
            padding: 12px;
            margin-top: 15px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
            transition: transform 0.2s;
        }

        .auth-button:active {
            transform: scale(0.98);
        }

        .auth-error {
            color: #e74c3c;
            text-align: center;
            margin-top: 10px;
            font-size: 14px;
        }

        /* Основной чат */
        .chat-container {
            display: flex;
            flex-direction: column;
            height: 100%;
            width: 100%;
            overflow: hidden;
            position: relative;
        }

        /* Верхняя панель */
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

        .chat-title h1 {
            font-size: 1.1em;
        }

        .online-status {
            background: #238636;
            color: white;
            padding: 3px 8px;
            border-radius: 20px;
            font-size: 0.7em;
        }

        .user-info {
            display: flex;
            gap: 8px;
            align-items: center;
            flex-wrap: wrap;
        }

        .username-display {
            background: #21262d;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
        }

        .toggle-users-btn {
            background: #21262d;
            border: 1px solid #30363d;
            color: #f0f6fc;
            padding: 5px 10px;
            border-radius: 20px;
            cursor: pointer;
            font-size: 0.8em;
            min-height: 34px;
        }

        /* Основная область */
        .chat-main {
            display: flex;
            flex: 1;
            overflow: hidden;
            min-height: 0;
        }

        /* Сайдбар */
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
            font-size: 14px;
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
            -webkit-overflow-scrolling: touch;
        }

        .user-item {
            padding: 8px 10px;
            margin: 2px 0;
            border-radius: 8px;
            display: flex;
            align-items: center;
            gap: 8px;
            cursor: pointer;
            min-height: 44px;
            position: relative;
        }

        .user-item:hover { background: #21262d; }
        .user-item.spam { background: #6e3a3a; opacity: 0.7; }
        .user-item.blocked { background: #8b0000; opacity: 0.5; cursor: not-allowed; }

        .user-avatar {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #238636;
        }

        .user-avatar.spam { background: #da3633; }
        .user-avatar.blocked { background: #ff0000; }
        .user-name { flex: 1; font-size: 14px; }
        
        .blocked-badge {
            font-size: 0.7em;
            padding: 2px 6px;
            border-radius: 10px;
            background: #8b0000;
            color: white;
        }

        /* Счётчик непрочитанных сообщений */
        .unread-badge {
            background: #e74c3c;
            color: white;
            border-radius: 20px;
            padding: 2px 6px;
            font-size: 0.7em;
            font-weight: bold;
            min-width: 20px;
            text-align: center;
            margin-left: 5px;
        }

        /* Область сообщений */
        .messages-area {
            flex: 1;
            min-height: 0;
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
            -webkit-overflow-scrolling: touch;
        }

        .chat-tab {
            padding: 6px 12px;
            background: #21262d;
            border: none;
            color: #8b949e;
            cursor: pointer;
            border-radius: 6px;
            white-space: nowrap;
            font-size: 14px;
            min-height: 34px;
            position: relative;
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

        /* Счётчик на вкладке */
        .tab-unread {
            background: #e74c3c;
            color: white;
            border-radius: 12px;
            padding: 2px 6px;
            font-size: 0.7em;
            margin-left: 6px;
        }

        /* Контейнер сообщений */
        .messages-container {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
            display: flex;
            flex-direction: column;
            gap: 10px;
            -webkit-overflow-scrolling: touch;
        }

        .messages-container::-webkit-scrollbar {
            width: 4px;
        }

        .messages-container::-webkit-scrollbar-track {
            background: #21262d;
        }

        .messages-container::-webkit-scrollbar-thumb {
            background: #58a6ff;
            border-radius: 4px;
        }

        /* Сообщения */
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

        /* Индикатор печатания */
        .typing-indicator {
            padding: 6px 16px;
            font-size: 0.75em;
            color: #8b949e;
            font-style: italic;
            min-height: 32px;
            background: #0d1117;
            flex-shrink: 0;
        }

        /* НИЖНЯЯ ПАНЕЛЬ ВВОДА */
        .input-area {
            background: #161b22;
            border-top: 1px solid #30363d;
            padding: 10px 12px;
            display: flex;
            gap: 8px;
            flex-shrink: 0;
            align-items: flex-end;
            position: relative;
            padding-bottom: max(10px, env(safe-area-inset-bottom));
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
            font-size: 16px;
        }

        .send-btn {
            background: #58a6ff;
            color: white;
            border: none;
            padding: 0 20px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: bold;
            min-height: 44px;
            position: relative;
        }

        /* Общий счётчик непрочитанных над кнопкой */
        .total-unread {
            position: absolute;
            top: -8px;
            right: -8px;
            background: #e74c3c;
            color: white;
            border-radius: 20px;
            padding: 2px 6px;
            font-size: 0.7em;
            font-weight: bold;
            min-width: 18px;
            text-align: center;
            box-shadow: 0 0 4px rgba(0,0,0,0.3);
        }

        @media (max-width: 768px) {
            .users-sidebar {
                width: 100%;
                position: absolute;
                left: 0;
                right: 0;
                top: 0;
                bottom: 0;
                z-index: 1000;
            }
            .message-bubble {
                max-width: 85%;
            }
            .chat-header {
                padding: 8px;
            }
            .user-info {
                gap: 4px;
            }
        }
    </style>
</head>
<body>
    <!-- Экран авторизации -->
    <div class="auth-screen" id="authScreen">
        <div class="auth-container">
            <h2>Вход в чат</h2>
            <input type="email" id="authEmail" class="auth-input" placeholder="Email" autocomplete="email">
            <input type="password" id="authPassword" class="auth-input" placeholder="Пароль" autocomplete="current-password">
            <button class="auth-button" id="authButton">Войти</button>
            <div class="auth-error" id="authError"></div>
        </div>
    </div>

    <!-- Основной чат -->
    <div class="chat-container" id="chatContainer" style="display: none;">
        <!-- ВЕРХНЯЯ ПАНЕЛЬ -->
        <div class="chat-header">
            <div class="chat-title">
                <h1>Защищённый чат</h1>
                <span class="online-status" id="onlineCount">0 онлайн</span>
            </div>
            <div class="user-info">
                <span class="username-display" id="currentUsername">Загрузка...</span>
                <button class="toggle-users-btn" id="toggleUsersBtn">Участники</button>
            </div>
        </div>

        <!-- ОСНОВНАЯ ОБЛАСТЬ -->
        <div class="chat-main">
            <!-- Сайдбар -->
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

            <!-- Область чата -->
            <div class="messages-area">
                <div class="chat-tabs" id="chatTabs">
                    <button class="chat-tab active" data-chat="main">Общий чат</button>
                </div>
                <div class="messages-container" id="messagesContainer"></div>
                <div class="typing-indicator" id="typingIndicator"></div>
            </div>
        </div>

        <!-- НИЖНЯЯ ПАНЕЛЬ ВВОДА -->
        <div class="input-area">
            <textarea id="messageInput" class="message-input" placeholder="Введите сообщение..."></textarea>
            <button class="send-btn" id="sendButton">Отправить</button>
        </div>
    </div>

    <script>
        // === МОБИЛЬНАЯ АДАПТАЦИЯ ===
        function setMobileHeight() {
            const vh = window.innerHeight * 0.01;
            document.documentElement.style.setProperty('--window-height', `${vh}px`);
        }
        
        setMobileHeight();
        window.addEventListener('resize', () => {
            setTimeout(setMobileHeight, 100);
            setTimeout(() => {
                const container = document.getElementById('messagesContainer');
                if (container) container.scrollTop = container.scrollHeight;
            }, 50);
        });
        
        // Определяем мобильное устройство
        const isMobile = 'ontouchstart' in window;
        
        // Блокируем скролл body
        document.body.addEventListener('touchmove', function(e) {
            if (e.target.closest('.messages-container') || 
                e.target.closest('.users-list') ||
                e.target.closest('.chat-tabs')) {
                return;
            }
            e.preventDefault();
        }, { passive: false });
        
        window.addEventListener('scroll', function() {
            if (window.scrollY !== 0) {
                window.scrollTo(0, 0);
            }
        });
        
        // === ПЕРЕМЕННЫЕ ===
        var ws = null;
        var currentUser = null;
        var currentEmail = null;
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
        var blockedUsers = [];
        
        // Счётчики непрочитанных сообщений
        var unreadCounts = {}; // { "имя_пользователя": количество }
        
        
        // DOM элементы
        var authScreen = document.getElementById('authScreen');
        var chatContainer = document.getElementById('chatContainer');
        var authEmail = document.getElementById('authEmail');
        var authPassword = document.getElementById('authPassword');
        var authButton = document.getElementById('authButton');
        var authError = document.getElementById('authError');
        var messagesContainer = document.getElementById('messagesContainer');
        var messageInput = document.getElementById('messageInput');
        var typingIndicator = document.getElementById('typingIndicator');
        var currentUsernameSpan = document.getElementById('currentUsername');
        var onlineCountSpan = document.getElementById('onlineCount');
        var usersCountSpan = document.getElementById('usersCount');
        var usersList = document.getElementById('usersList');
        var usersSidebar = document.getElementById('usersSidebar');
        var sendButton = document.getElementById('sendButton');
        var toggleUsersBtn = document.getElementById('toggleUsersBtn');
        var userSearch = document.getElementById('userSearch');
        var totalUnreadSpan = document.getElementById('totalUnread');
        
        window.messagesHistory = [];
        

        
        function incrementUnreadCount(fromUser) {
            if (fromUser === currentUser) return;
            if (currentChat === fromUser) return; // Если чат открыт, не считаем
            
            if (!unreadCounts[fromUser]) {
                unreadCounts[fromUser] = 0;
            }
            unreadCounts[fromUser]++;
            
            
            
            updateUsersListDisplay();
            updateTabUnread(fromUser);
        }
        
        function clearUnreadCount(user) {
            if (unreadCounts[user]) {
                
                unreadCounts[user] = 0;
                
                updateUsersListDisplay();
                updateTabUnread(user);
            }
        }
        
        function updateTabUnread(user) {
            var tabs = document.querySelectorAll('.chat-tab');
            for (var i = 0; i < tabs.length; i++) {
                var tab = tabs[i];
                var tabUser = tab.getAttribute('data-chat');
                if (tabUser === user) {
                    // Удаляем старый счётчик
                    var oldSpan = tab.querySelector('.tab-unread');
                    if (oldSpan) oldSpan.remove();
                    
                    // Добавляем новый если есть
                    var count = unreadCounts[user] || 0;
                    if (count > 0) {
                        var span = document.createElement('span');
                        span.className = 'tab-unread';
                        span.textContent = count > 99 ? '99+' : count;
                        tab.appendChild(span);
                    }
                    break;
                }
            }
        }
        
        function updateUsersListDisplay() {
            // Обновляем отображение списка пользователей с учётом счётчиков
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
                var isBlocked = isBlockedUser(user.name);
                var sessionsHtml = (user.sessions > 1) ? ' (' + user.sessions + ' вкладки)' : '';
                var spamCount = spamStats[user.name] || 0;
                var statsHtml = spamCount > 0 ? ' ⚠️' + spamCount : '';
                var unreadCount = unreadCounts[user.name] || 0;
                var unreadHtml = (unreadCount > 0 && !isCurrent) ? '<span class="unread-badge">' + (unreadCount > 99 ? '99+' : unreadCount) + '</span>' : '';
                
                var onClick = (isCurrent || isBlocked) ? '' : ' onclick="startPrivateChat(\'' + escapeHtml(user.name) + '\')"';
                var onSpam = !isCurrent && !isBlocked ? ' onclick="event.stopPropagation(); toggleSpam(\'' + escapeHtml(user.name) + '\')"' : '';
                var badgeText = isSpam ? 'Снять спам' : 'Спам';
                var badgeClass = isSpam ? 'spam-badge' : 'private-badge';
                var blockedHtml = isBlocked ? '<span class="blocked-badge">🔒 Заблокирован</span>' : '';
                
                var userClass = 'user-item';
                if (isSpam) userClass += ' spam';
                if (isBlocked) userClass += ' blocked';
                
                html += '<div class="' + userClass + '"' + onClick + '>';
                html += '<div class="user-avatar ' + (isSpam ? 'spam' : '') + (isBlocked ? ' blocked' : '') + '"></div>';
                html += '<div class="user-name">' + escapeHtml(user.name) + (isCurrent ? ' (Вы)' : '') + sessionsHtml + statsHtml + '</div>';
                html += unreadHtml;
                if (!isCurrent && !isBlocked) html += '<span class="' + badgeClass + '"' + onSpam + '>' + badgeText + '</span>';
                if (isBlocked) html += blockedHtml;
                html += '</div>';
            }
            usersList.innerHTML = html;
        }
        
        // === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
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
            setTimeout(() => {
                messagesContainer.scrollTop = messagesContainer.scrollHeight;
            }, 10);
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
            tab.innerHTML = username;
            
            // Добавляем счётчик если есть
            var count = unreadCounts[username] || 0;
            if (count > 0) {
                var span = document.createElement('span');
                span.className = 'tab-unread';
                span.textContent = count > 99 ? '99+' : count;
                tab.appendChild(span);
            }
            
            tab.onclick = function(e) {
                if (e.target.className !== 'close-tab') window.switchChat(username);
            };
            
            // Добавляем крестик
            var closeSpan = document.createElement('span');
            closeSpan.className = 'close-tab';
            closeSpan.textContent = '✖';
            closeSpan.onclick = function(e) {
                e.stopPropagation();
                window.closePrivateChat(username);
            };
            tab.appendChild(closeSpan);
            
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
            // Если переключаемся на приватный чат - сбрасываем счётчик
            if (chatId !== 'main') {
                clearUnreadCount(chatId);
            }
            
            currentChat = chatId;
            var tabs = document.querySelectorAll('.chat-tab');
            for (var i = 0; i < tabs.length; i++) {
                var tab = tabs[i];
                var tabChat = tab.getAttribute('data-chat');
                if ((chatId === 'main' && tabChat === 'main') || (chatId !== 'main' && tabChat === chatId)) {
                    tab.classList.add('active');
                    // Сбрасываем фон уведомления
                    tab.style.background = '';
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
            if (blockedUsers.includes(username)) {
                showSystemMessage('⚠️ Невозможно начать чат с заблокированным пользователем');
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
            for (var i = 0; i < spamList.length; i++) {
                if (spamList[i] === username) return true;
            }
            return false;
        }
        
        function isBlockedUser(username) {
            for (var i = 0; i < blockedUsers.length; i++) {
                if (blockedUsers[i] === username) return true;
            }
            return false;
        }
        
        window.setUserFilter = function(filter) {
            currentUserFilter = filter;
            var btns = document.querySelectorAll('.filter-btn');
            for (var i = 0; i < btns.length; i++) {
                var btn = btns[i];
                btn.classList.remove('active');
                if (btn.getAttribute('data-filter') === filter) btn.classList.add('active');
            }
            updateUsersListDisplay();
        };
        
        window.toggleUsers = function() {
            if (usersSidebar.classList.contains('show')) {
                usersSidebar.classList.remove('show');
            } else {
                usersSidebar.classList.add('show');
                updateUsersListDisplay();
            }
        };
        
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
            updateUsersListDisplay();
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
            // Сбрасываем высоту
            messageInput.style.height = 'auto';
            setTimeout(scrollToBottom, 10);
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
            
            if (!isFromMe && isBlockedUser(message.from)) {
                return;
            }
            if (!isFromMe && isSpam) return;
            
            // Увеличиваем счётчик непрочитанных, если не от себя и не текущий чат
            if (!isFromMe && currentChat !== message.from) {
                incrementUnreadCount(message.from);
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
        
        function updateUsersList(users, count) {
            allUsers = users;
            usersCountSpan.textContent = count;
            onlineCountSpan.textContent = count + ' онлайн';
            updateUsersListDisplay();
        }
        
        function handleMessage(data) {
            switch(data.type) {
                case 'message':
                    if (currentChat === 'main') {
                        addMessageToChat(data);
                        window.messagesHistory.push(data);
                        if (window.messagesHistory.length > 100) window.messagesHistory.shift();
                    }
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
                    updateUsersListDisplay();
                    break;
                case 'spam_stats':
                    spamStats = data.stats || {};
                    updateUsersListDisplay();
                    break;
            }
        }
        
        // === ПОДКЛЮЧЕНИЕ К ЧАТУ ===
        function connectToChat(email, username) {
            sessionId = getSessionId();
            var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            var url = protocol + '//' + window.location.host + '/ws';
            ws = new WebSocket(url);
            
            ws.onopen = function() {
                ws.send(JSON.stringify({ 
                    email: email,
                    username: username,
                    session_id: sessionId 
                }));
                
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
                setTimeout(function() { 
                    if (currentUser) connectToChat(currentEmail, currentUser); 
                }, 3000);
            };
        }
        
        // === АВТОРИЗАЦИЯ ===
        async function login() {
            var email = authEmail.value.trim();
            var password = authPassword.value;
            
            if (!email || !password) {
                authError.textContent = 'Введите email и пароль';
                return;
            }
            
            authButton.disabled = true;
            authError.textContent = '';
            
            try {
                var response = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email: email, password: password })
                });
                
                var result = await response.json();
                
                if (result.success) {
                    currentEmail = email;
                    currentUser = result.username;
                    
                    authScreen.classList.add('hidden');
                    setTimeout(function() {
                        authScreen.style.display = 'none';
                        chatContainer.style.display = 'flex';
                        currentUsernameSpan.textContent = currentUser;
                        
                        connectToChat(currentEmail, currentUser);
                        
                        messageInput.focus();
                    }, 300);
                } else {
                    authError.textContent = result.message || 'Неверный email или пароль';
                    authButton.disabled = false;
                }
            } catch (error) {
                authError.textContent = 'Ошибка соединения с сервером';
                authButton.disabled = false;
            }
        }
        
        // === ОБРАБОТЧИКИ СОБЫТИЙ ===
        authButton.onclick = login;
        
        authEmail.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') login();
        });
        
        authPassword.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') login();
        });
        
        sendButton.onclick = window.sendMessage;
        toggleUsersBtn.onclick = window.toggleUsers;
        userSearch.onkeyup = function() { updateUsersListDisplay(); };
        
        // Поведение клавиши Enter в textarea
        messageInput.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                if (isMobile) {
                    // На мобильных - новая строка (ничего не делаем, стандартное поведение)
                    return;
                } else {
                    // На десктопе: если без Shift - отправка
                    if (!e.shiftKey) {
                        e.preventDefault();
                        window.sendMessage();
                    }
                    // С Shift - новая строка (стандартное поведение)
                }
            }
        });
        
        // Автоматическое изменение высоты textarea
        messageInput.addEventListener('input', function() {
            this.style.height = 'auto';
            this.style.height = Math.min(this.scrollHeight, 120) + 'px';
        });
        
        // Индикатор печатания
        messageInput.addEventListener('input', function() {
            if (!isTyping) {
                isTyping = true;
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'typing', is_typing: true }));
                }
            }
            clearTimeout(typingTimeout);
            typingTimeout = setTimeout(function() {
                if (isTyping) {
                    isTyping = false;
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'typing', is_typing: false }));
                    }
                }
            }, 1000);
        });
        
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
        
        window.addMessageToChat = addMessageToChat;
    </script>
</body>
</html>'''

# === API ЭНДПОЙНТЫ ===

async def handle_index(request):
    """Главная страница"""
    return web.Response(text=HTML_PAGE, content_type='text/html')

async def handle_login(request):
    """API для авторизации"""
    try:
        data = await request.json()
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        
        if not email or not password:
            return web.json_response({'success': False, 'message': 'Введите email и пароль'})
        
        success, message = user_auth.verify_password(email, password)
        
        if success:
            username = user_auth.get_user_name(email)
            return web.json_response({
                'success': True, 
                'username': username,
                'email': email
            })
        else:
            return web.json_response({'success': False, 'message': message})
    except Exception as e:
        print(f"Login error: {e}")
        return web.json_response({'success': False, 'message': 'Ошибка сервера'})

async def websocket_handler(request):
    """WebSocket обработчик"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        msg = await ws.receive()
        if msg.type != web.WSMsgType.TEXT:
            await ws.close()
            return ws

        data = json.loads(msg.data)
        email = data.get('email', '').strip().lower()
        username = data.get('username', '').strip()
        session_id = data.get('session_id', '')
        
        if user_auth.is_user_blocked(email):
            await ws.send_str(json.dumps({
                'type': 'system',
                'message': '❌ Ваш аккаунт заблокирован. Обратитесь к администратору.'
            }))
            await ws.close()
            return ws
        
        if not username:
            username = user_auth.get_user_name(email)
        
        username = username[:20]
        
        if not session_id:
            session_id = f"session_{datetime.now().timestamp()}"

        await chat_processor.register(ws, email, username, session_id)

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
    """Проверка работоспособности"""
    return web.Response(text="OK")

async def sync_users(request):
    """Эндпоинт для ручной синхронизации"""
    user_auth.sync_from_github()
    return web.json_response({'success': True, 'message': 'Синхронизация выполнена'})

# === ЗАПУСК ===
app = web.Application()
app.router.add_get('/', handle_index)
app.router.add_post('/api/login', handle_login)
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/healthz', health_check)
app.router.add_post('/api/sync', sync_users)

if __name__ == "__main__":
    print(f"🚀 Сервер запущен на порту {PORT}")
    print(f"📧 Авторизация через email и пароль")
    print(f"📁 Файл пользователей: {USERS_FILE}")
    print(f"🔒 Блокировка пользователей поддерживается")
    print(f"👤 Имена пользователей фиксированы (берутся из JSON)")
    print(f"📱 На мобильных Enter - новая строка, кнопка - отправка")
    web.run_app(app, host='0.0.0.0', port=PORT)
