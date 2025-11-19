#!/usr/bin/env python3
import asyncio
import websockets
import json
import random
import socket
import logging
import base64
import sqlite3
import datetime
import math  # Добавлен импорт math
from typing import Dict, Set, List, Optional
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PacmanServer')


class DatabaseManager:
    """Менеджер базы данных для рейтингов и статистики"""

    def __init__(self):
        self.conn = sqlite3.connect('pacman_ratings.db', check_same_thread=False)
        self.create_tables()

    def create_tables(self):
        """Создание таблиц в базе данных"""
        cursor = self.conn.cursor()

        # Таблица игроков
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица рейтингов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                score INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                best_score INTEGER DEFAULT 0,
                last_played TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
        ''')

        # Таблица достижений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                achievement_name TEXT NOT NULL,
                achieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
        ''')

        self.conn.commit()

    def get_player_rating(self, username: str) -> Dict:
        """Получить рейтинг игрока"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT r.score, r.games_played, r.wins, r.best_score 
            FROM ratings r 
            JOIN players p ON r.player_id = p.id 
            WHERE p.username = ?
        ''', (username,))

        result = cursor.fetchone()
        if result:
            return {
                'score': result[0],
                'games_played': result[1],
                'wins': result[2],
                'best_score': result[3]
            }
        return {'score': 0, 'games_played': 0, 'wins': 0, 'best_score': 0}

    def update_player_rating(self, username: str, score: int, is_win: bool):
        """Обновить рейтинг игрока"""
        cursor = self.conn.cursor()

        # Создаем игрока если не существует
        cursor.execute('INSERT OR IGNORE INTO players (username) VALUES (?)', (username,))

        # Получаем ID игрока
        cursor.execute('SELECT id FROM players WHERE username = ?', (username,))
        player_id = cursor.fetchone()[0]

        # Получаем текущий рейтинг
        cursor.execute('SELECT * FROM ratings WHERE player_id = ?', (player_id,))
        current_rating = cursor.fetchone()

        if current_rating:
            # Обновляем существующий рейтинг
            new_score = current_rating[2] + score
            new_games = current_rating[3] + 1
            new_wins = current_rating[4] + (1 if is_win else 0)
            new_best_score = max(current_rating[5], score)

            cursor.execute('''
                UPDATE ratings 
                SET score = ?, games_played = ?, wins = ?, best_score = ?, last_played = CURRENT_TIMESTAMP
                WHERE player_id = ?
            ''', (new_score, new_games, new_wins, new_best_score, player_id))
        else:
            # Создаем новый рейтинг
            cursor.execute('''
                INSERT INTO ratings (player_id, score, games_played, wins, best_score)
                VALUES (?, ?, 1, ?, ?)
            ''', (player_id, score, 1 if is_win else 0, score))

        self.conn.commit()

    def get_leaderboard(self, limit: int = 10) -> List[Dict]:
        """Получить таблицу лидеров"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT p.username, r.score, r.games_played, r.wins, r.best_score
            FROM ratings r
            JOIN players p ON r.player_id = p.id
            ORDER BY r.score DESC
            LIMIT ?
        ''', (limit,))

        leaderboard = []
        for row in cursor.fetchall():
            leaderboard.append({
                'username': row[0],
                'score': row[1],
                'games_played': row[2],
                'wins': row[3],
                'best_score': row[4]
            })

        return leaderboard

    def add_achievement(self, username: str, achievement_name: str):
        """Добавить достижение игроку"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM players WHERE username = ?', (username,))
        result = cursor.fetchone()

        if result:
            player_id = result[0]
            cursor.execute('''
                INSERT INTO achievements (player_id, achievement_name)
                VALUES (?, ?)
            ''', (player_id, achievement_name))
            self.conn.commit()


class WebSocketPacmanServer:
    def __init__(self, host: str = 'localhost', port: int = 5556, name: str = 'WinterPacmanServer'):
        self.host = host
        self.port = port
        self.server_name = name
        self.connected_clients: Set[websockets.WebSocketServerProtocol] = set()
        self.players: Dict[str, Dict] = {}
        self.pacman_player_id: Optional[str] = None
        self.player_counter = 0
        self.db = DatabaseManager()

        # Зимние цвета
        self.ghost_colors_available = [
            [173, 216, 230],  # Голубой (лед)
            [255, 182, 193],  # Розовый (зимний рассвет)
            [152, 251, 152],  # Светло-зеленый (северное сияние)
            [221, 160, 221],  # Фиолетовый (сумерки)
            [240, 248, 255],  # Белоснежный
            [176, 224, 230],  # Пудрово-голубой
            [255, 250, 205]  # Снежно-желтый
        ]
        self.used_ghost_colors: Set[tuple] = set()

        # Игровое поле и карты
        self.current_map = 0
        self.maps = self.generate_winter_maps()

        # Таймер восстановления очков
        self.dot_respawn_interval = 30
        self.last_respawn_time = datetime.now()

        # Голосовой чат
        self.voice_chat_enabled = True
        self.voice_data_buffer = {}

        # Инициализация текущей карты
        self.dots = self.maps[self.current_map]['dots']
        self.power_pellets = self.maps[self.current_map]['power_pellets']
        self.walls = self.maps[self.current_map]['walls']
        self.snowflakes = self.maps[self.current_map]['snowflakes']

        print(f"🎮 WebSocket Winter Pacman Server - {name}")
        print(f"📍 Хост: {self.host}")
        print(f"🚪 Порт: {self.port}")
        print(f"🗺️  Текущая карта: {self.maps[self.current_map]['name']}")
        print("🎯 Один игрок - Пакмен, остальные - снежные призраки!")
        print("🔄 Авто-восстановление снежинок: каждые 30 секунд")
        print("🎤 Голосовой чат: ВКЛЮЧЕН")
        print("🏆 Система рейтингов: АКТИВНА")
        print("🌙 Ночная зимняя тематика")
        print("=" * 50)

    def generate_snowflakes(self, count: int = 50):
        """Генерация снежинок для зимней тематики"""
        snowflakes = []
        for _ in range(count):
            snowflakes.append({
                'x': random.randint(50, 950),
                'y': random.randint(50, 650),
                'size': random.randint(2, 4),
                'speed': random.uniform(0.5, 2.0),
                'brightness': random.uniform(0.7, 1.0)
            })
        return snowflakes

    def generate_winter_maps(self):
        """Генерация 5 зимних карт"""
        maps = []

        # Карта 1: Зимний лес
        maps.append({
            'name': 'Winter Forest',
            'walls': self.generate_forest_walls(),
            'dots': self.generate_snowflakes_for_map(1),
            'power_pellets': self.generate_icicles_for_map(1),
            'snowflakes': self.generate_snowflakes(80),
            'pacman_spawn': (400, 500),
            'ghost_spawns': [(400, 300), (300, 300), (500, 300), (400, 200)],
            'background': 'night_forest'
        })

        # Карта 2: Ледяной лабиринт
        maps.append({
            'name': 'Ice Maze',
            'walls': self.generate_ice_maze_walls(),
            'dots': self.generate_snowflakes_for_map(2),
            'power_pellets': self.generate_icicles_for_map(2),
            'snowflakes': self.generate_snowflakes(60),
            'pacman_spawn': (100, 100),
            'ghost_spawns': [(800, 600), (800, 100), (100, 600), (450, 350)],
            'background': 'ice_cave'
        })

        # Карта 3: Северное сияние
        maps.append({
            'name': 'Aurora Circle',
            'walls': self.generate_aurora_walls(),
            'dots': self.generate_snowflakes_for_map(3),
            'power_pellets': self.generate_icicles_for_map(3),
            'snowflakes': self.generate_snowflakes(100),
            'pacman_spawn': (100, 350),
            'ghost_spawns': [(800, 350), (450, 100), (450, 600), (450, 350)],
            'background': 'aurora'
        })

        # Карта 4: Заснеженная деревня
        maps.append({
            'name': 'Snow Village',
            'walls': self.generate_village_walls(),
            'dots': self.generate_snowflakes_for_map(4),
            'power_pellets': self.generate_icicles_for_map(4),
            'snowflakes': self.generate_snowflakes(70),
            'pacman_spawn': (150, 150),
            'ghost_spawns': [(750, 550), (750, 150), (150, 550), (450, 350)],
            'background': 'snow_village'
        })

        # Карта 5: Ледяная спираль
        maps.append({
            'name': 'Ice Spiral',
            'walls': self.generate_ice_spiral_walls(),
            'dots': self.generate_snowflakes_for_map(5),
            'power_pellets': self.generate_icicles_for_map(5),
            'snowflakes': self.generate_snowflakes(90),
            'pacman_spawn': (450, 450),
            'ghost_spawns': [(100, 100), (800, 100), (100, 600), (800, 600)],
            'background': 'frozen_lake'
        })

        return maps

    def generate_forest_walls(self):
        """Стены зимнего леса"""
        walls = []
        # Внешние стены (снежные сугробы)
        for x in range(50, 950, 60):
            walls.append({'x': x, 'y': 50, 'width': 50, 'height': 25, 'color': [240, 248, 255]})
            walls.append({'x': x, 'y': 650, 'width': 50, 'height': 25, 'color': [240, 248, 255]})
        for y in range(50, 650, 60):
            walls.append({'x': 50, 'y': y, 'width': 25, 'height': 50, 'color': [240, 248, 255]})
            walls.append({'x': 925, 'y': y, 'width': 25, 'height': 50, 'color': [240, 248, 255]})

        # Снежные ели
        trees = [
            (200, 150, 40, 80), (600, 150, 40, 80),
            (300, 400, 40, 80), (700, 400, 40, 80),
            (150, 300, 30, 60), (750, 300, 30, 60)
        ]

        for x, y, width, height in trees:
            walls.append({'x': x, 'y': y, 'width': width, 'height': height, 'color': [34, 139, 34]})

        return walls

    def generate_ice_maze_walls(self):
        """Ледяные стены лабиринта"""
        walls = []
        # Внешние ледяные стены
        for x in range(50, 950, 50):
            walls.append({'x': x, 'y': 50, 'width': 40, 'height': 20, 'color': [173, 216, 230]})
            walls.append({'x': x, 'y': 650, 'width': 40, 'height': 20, 'color': [173, 216, 230]})
        for y in range(50, 650, 50):
            walls.append({'x': 50, 'y': y, 'width': 20, 'height': 40, 'color': [173, 216, 230]})
            walls.append({'x': 930, 'y': y, 'width': 20, 'height': 40, 'color': [173, 216, 230]})

        # Ледяные перегородки
        ice_pattern = [
            (200, 100, 300, 15), (600, 100, 15, 200),
            (100, 300, 250, 15), (650, 300, 250, 15),
            (300, 400, 15, 150), (500, 400, 15, 150),
            (200, 500, 200, 15), (600, 500, 200, 15)
        ]

        for x, y, width, height in ice_pattern:
            walls.append({'x': x, 'y': y, 'width': width, 'height': height, 'color': [135, 206, 250]})

        return walls

    def generate_aurora_walls(self):
        """Стены с северным сиянием"""
        walls = []
        # Круговая арена
        for angle in range(0, 360, 30):
            rad = math.radians(angle)
            x = 450 + 300 * math.cos(rad)
            y = 350 + 200 * math.sin(rad)
            walls.append({
                'x': x - 20, 'y': y - 10,
                'width': 40, 'height': 20,
                'color': [random.randint(0, 100), random.randint(100, 255), random.randint(150, 255)]
            })

        # Внутренние перегородки
        inner_walls = [
            (350, 250, 15, 80), (550, 250, 15, 80),
            (350, 450, 15, 80), (550, 450, 15, 80),
            (400, 300, 100, 15), (400, 400, 100, 15)
        ]

        for x, y, width, height in inner_walls:
            walls.append({'x': x, 'y': y, 'width': width, 'height': height, 'color': [72, 61, 139]})

        return walls

    def generate_village_walls(self):
        """Стены заснеженной деревни"""
        walls = []
        # Дома
        houses = [
            (100, 100, 120, 100), (700, 100, 120, 100),
            (100, 450, 120, 100), (700, 450, 120, 100),
            (350, 280, 150, 120)
        ]

        for x, y, width, height in houses:
            # Стены дома
            walls.append({'x': x, 'y': y, 'width': width, 'height': height, 'color': [139, 69, 19]})
            # Снег на крыше
            walls.append({'x': x - 10, 'y': y - 15, 'width': width + 20, 'height': 15, 'color': [240, 248, 255]})

        # Деревья
        trees = [
            (250, 150, 25, 60), (550, 150, 25, 60),
            (250, 500, 25, 60), (550, 500, 25, 60)
        ]

        for x, y, width, height in trees:
            walls.append({'x': x, 'y': y, 'width': width, 'height': height, 'color': [34, 139, 34]})

        return walls

    def generate_ice_spiral_walls(self):
        """Ледяные спиральные стены"""
        walls = []
        # Спиральные ледяные стены
        spiral_coords = [
            (100, 100, 700, 15), (100, 100, 15, 500),
            (100, 600, 700, 15), (800, 100, 15, 500),
            (150, 150, 600, 15), (150, 150, 15, 400),
            (150, 550, 600, 15), (750, 150, 15, 400),
            (200, 200, 500, 15), (200, 200, 15, 300),
            (200, 500, 500, 15), (700, 200, 15, 300)
        ]

        for i, (x, y, width, height) in enumerate(spiral_coords):
            # Градиент цвета от светлого к темному
            blue_shade = 230 - i * 15
            walls.append({'x': x, 'y': y, 'width': width, 'height': height, 'color': [173, 216, blue_shade]})

        return walls

    def generate_snowflakes_for_map(self, map_id):
        """Генерация снежинок для конкретной карты"""
        snowflakes = []
        count = [120, 100, 150, 110, 130][map_id - 1]  # Разное количество для каждой карты

        for _ in range(count):
            snowflakes.append({
                'x': random.randint(80, 920),
                'y': random.randint(80, 620),
                'size': random.randint(2, 5),
                'brightness': random.uniform(0.6, 1.0),
                'type': random.choice(['regular', 'crystal', 'star'])
            })
        return snowflakes

    def generate_icicles_for_map(self, map_id):
        """Генерация сосулек (силовые точки)"""
        if map_id == 1:
            positions = [(120, 120), (880, 120), (120, 580), (880, 580)]
        elif map_id == 2:
            positions = [(180, 180), (820, 180), (180, 520), (820, 520)]
        elif map_id == 3:
            positions = [(200, 200), (700, 200), (200, 500), (700, 500)]
        elif map_id == 4:
            positions = [(250, 250), (750, 250), (250, 450), (750, 450)]
        else:
            positions = [(200, 200), (700, 200), (200, 500), (700, 500)]

        return [{'x': x, 'y': y, 'eaten': False} for x, y in positions]

    def check_wall_collision(self, x: int, y: int, player_size: int = 30) -> bool:
        """Проверка столкновения со стенами"""
        player_rect = {
            'left': x - player_size // 2,
            'right': x + player_size // 2,
            'top': y - player_size // 2,
            'bottom': y + player_size // 2
        }

        for wall in self.walls:
            wall_rect = {
                'left': wall['x'],
                'right': wall['x'] + wall['width'],
                'top': wall['y'],
                'bottom': wall['y'] + wall['height']
            }

            # Проверка пересечения прямоугольников
            if (player_rect['right'] > wall_rect['left'] and
                    player_rect['left'] < wall_rect['right'] and
                    player_rect['bottom'] > wall_rect['top'] and
                    player_rect['top'] < wall_rect['bottom']):
                return True

        return False

    def get_valid_position(self, old_x: int, old_y: int, new_x: int, new_y: int, player_size: int = 30) -> tuple:
        """Получить валидную позицию с учетом стен"""
        # Сначала проверяем новую позицию
        if not self.check_wall_collision(new_x, new_y, player_size):
            return new_x, new_y

        # Если новая позиция невалидна, пробуем двигаться только по X
        if not self.check_wall_collision(new_x, old_y, player_size):
            return new_x, old_y

        # Пробуем двигаться только по Y
        if not self.check_wall_collision(old_x, new_y, player_size):
            return old_x, new_y

        # Если ничего не помогает, остаемся на месте
        return old_x, old_y

    def get_ghost_color(self) -> List[int]:
        """Получение уникального зимнего цвета для призрака"""
        available_colors = [c for c in self.ghost_colors_available
                            if tuple(c) not in self.used_ghost_colors]
        if available_colors:
            color = available_colors[0]
            self.used_ghost_colors.add(tuple(color))
            return color
        else:
            return random.choice(self.ghost_colors_available)

    def assign_roles(self):
        """Назначение ролей игрокам"""
        if not self.players:
            return

        player_ids = list(self.players.keys())

        # Если Пакмена нет, назначаем случайного игрока
        if self.pacman_player_id is None or self.pacman_player_id not in self.players:
            if player_ids:
                self.pacman_player_id = random.choice(player_ids)
                logger.info(f"🎯 Игрок {self.pacman_player_id} стал Снежным Пакменом!")

        # Назначаем роли и цвета
        for player_id in player_ids:
            if player_id == self.pacman_player_id:
                self.players[player_id]['role'] = 'pacman'
                self.players[player_id]['color'] = [255, 255, 0]  # Желтый (снежный шар)
                if 'lives' not in self.players[player_id]:
                    self.players[player_id]['lives'] = 3
                if 'score' not in self.players[player_id]:
                    self.players[player_id]['score'] = 0
            else:
                self.players[player_id]['role'] = 'ghost'
                if 'color' not in self.players[player_id]:
                    self.players[player_id]['color'] = self.get_ghost_color()

    async def handle_client(self, websocket: websockets.WebSocketServerProtocol, path: str = None):
        """Обработка подключения клиента"""
        self.player_counter += 1
        player_id = str(self.player_counter)

        # Регистрируем клиента
        self.connected_clients.add(websocket)

        # Начальная позиция в зависимости от роли
        current_map = self.maps[self.current_map]

        self.players[player_id] = {
            'x': current_map['pacman_spawn'][0],
            'y': current_map['pacman_spawn'][1],
            'role': 'ghost',
            'color': self.get_ghost_color(),
            'score': 0,
            'power_mode': False,
            'power_timer': 0,
            'lives': 3,
            'websocket': websocket,
            'name': f'Player{player_id}',
            'voice_chat': True,
            'muted': False,
            'total_score': 0,
            'games_played': 0,
            'wins': 0
        }

        # Перераспределяем роли
        self.assign_roles()

        logger.info(f"🎮 Подключен игрок {player_id}")

        try:
            # Отправляем начальное состояние
            await self.send_game_state(player_id)

            # Обрабатываем сообщения от клиента
            async for message in websocket:
                await self.handle_message(player_id, message)

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"🔌 Игрок {player_id} отключился")
        except Exception as e:
            logger.error(f"❌ Ошибка с игроком {player_id}: {e}")
        finally:
            # Очистка при отключении
            await self.cleanup_player(player_id)

    async def handle_message(self, player_id: str, message: str):
        """Обработка сообщения от клиента"""
        try:
            data = json.loads(message)

            if data['type'] == 'position':
                new_x = data['position']['x']
                new_y = data['position']['y']

                # Обновляем имя если передано
                if 'name' in data['position']:
                    self.players[player_id]['name'] = data['position']['name']

                # Получаем старые координаты
                old_x = self.players[player_id]['x']
                old_y = self.players[player_id]['y']

                # Проверяем столкновение со стенами и получаем валидную позицию
                valid_x, valid_y = self.get_valid_position(old_x, old_y, new_x, new_y)

                # Обновляем позицию
                self.players[player_id]['x'] = valid_x
                self.players[player_id]['y'] = valid_y

                # Проверяем столкновения (только для Пакмена)
                if self.players[player_id]['role'] == 'pacman':
                    await self.check_snowflake_collision(player_id, valid_x, valid_y)
                    await self.check_icicle_collision(player_id, valid_x, valid_y)
                    await self.check_ghost_collision(player_id, valid_x, valid_y)

                # Обновляем таймер силы
                if self.players[player_id]['power_mode']:
                    self.players[player_id]['power_timer'] -= 1
                    if self.players[player_id]['power_timer'] <= 0:
                        self.players[player_id]['power_mode'] = False

            elif data['type'] == 'voice_chat':
                # Включение/выключение голосового чата
                self.players[player_id]['voice_chat'] = data['enabled']
                logger.info(f"🎤 Игрок {player_id} {'включил' if data['enabled'] else 'выключил'} голосовой чат")

            elif data['type'] == 'voice_audio':
                # Пересылка голосовых данных другим игрокам
                if self.players[player_id]['voice_chat'] and not self.players[player_id]['muted']:
                    await self.broadcast_voice_audio(player_id, data['audio_data'], data['sequence'])

            elif data['type'] == 'mute_player':
                # Заглушить/разглушить игрока
                target_player = data['player_id']
                if target_player in self.players:
                    self.players[target_player]['muted'] = data['muted']
                    logger.info(f"🔇 Игрок {player_id} {'заглушил' if data['muted'] else 'разглушил'} {target_player}")

            elif data['type'] == 'change_map':
                # Смена карты
                new_map = data['map_id']
                if 0 <= new_map < len(self.maps):
                    self.current_map = new_map
                    self.dots = self.maps[self.current_map]['dots']
                    self.power_pellets = self.maps[self.current_map]['power_pellets']
                    self.walls = self.maps[self.current_map]['walls']
                    self.snowflakes = self.maps[self.current_map]['snowflakes']

                    # Перемещаем всех игроков на новые позиции спавна
                    current_map_data = self.maps[self.current_map]
                    for pid, player in self.players.items():
                        if player['role'] == 'pacman':
                            player['x'], player['y'] = current_map_data['pacman_spawn']
                        else:
                            spawn_pos = random.choice(current_map_data['ghost_spawns'])
                            player['x'], player['y'] = spawn_pos

                    logger.info(f"🗺️ Смена карты на: {self.maps[self.current_map]['name']}")

            elif data['type'] == 'get_leaderboard':
                # Отправка таблицы лидеров
                leaderboard = self.db.get_leaderboard()
                await self.send_leaderboard(player_id, leaderboard)

            elif data['type'] == 'get_player_stats':
                # Отправка статистики игрока
                if 'username' in data:
                    stats = self.db.get_player_rating(data['username'])
                    await self.send_player_stats(player_id, stats)

            # Рассылаем обновленное состояние всем клиентам
            await self.broadcast_game_state()

        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка JSON от игрока {player_id}: {e}")

    async def send_leaderboard(self, player_id: str, leaderboard: List[Dict]):
        """Отправка таблицы лидеров игроку"""
        if player_id in self.players:
            message = {
                'type': 'leaderboard',
                'leaderboard': leaderboard
            }
            try:
                await self.players[player_id]['websocket'].send(json.dumps(message))
            except:
                pass

    async def send_player_stats(self, player_id: str, stats: Dict):
        """Отправка статистики игроку"""
        if player_id in self.players:
            message = {
                'type': 'player_stats',
                'stats': stats
            }
            try:
                await self.players[player_id]['websocket'].send(json.dumps(message))
            except:
                pass

    async def broadcast_voice_audio(self, sender_id: str, audio_data: str, sequence: int):
        """Рассылка голосовых данных другим игрокам"""
        tasks = []
        for player_id, player_data in self.players.items():
            if (player_id != sender_id and
                    player_data['voice_chat'] and
                    not player_data['muted'] and
                    player_data['websocket'] in self.connected_clients):

                message = {
                    'type': 'voice_audio',
                    'sender_id': sender_id,
                    'sender_name': self.players[sender_id]['name'],
                    'audio_data': audio_data,
                    'sequence': sequence
                }

                try:
                    tasks.append(
                        player_data['websocket'].send(json.dumps(message))
                    )
                except:
                    pass

        # Отправляем всем подходящим клиентам
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def check_snowflake_respawn(self):
        """Проверка и восстановление снежинок"""
        now = datetime.now()
        if (now - self.last_respawn_time).total_seconds() >= self.dot_respawn_interval:
            respawned = 0
            for snowflake in self.dots:
                if snowflake.get('eaten', False) and random.random() > 0.7:
                    snowflake['eaten'] = False
                    respawned += 1

            for icicle in self.power_pellets:
                if icicle['eaten'] and random.random() > 0.5:
                    icicle['eaten'] = False
                    respawned += 1

            if respawned > 0:
                logger.info(f"🔄 Восстановлено {respawned} снежинок")
                self.last_respawn_time = now

    async def check_snowflake_collision(self, player_id: str, x: int, y: int):
        """Проверка столкновения со снежинкой"""
        pacman_left, pacman_top = x - 15, y - 15
        pacman_right, pacman_bottom = x + 15, y + 15

        for snowflake in self.dots:
            if not snowflake.get('eaten', False):
                snowflake_left = snowflake['x'] - snowflake['size']
                snowflake_right = snowflake['x'] + snowflake['size']
                snowflake_top = snowflake['y'] - snowflake['size']
                snowflake_bottom = snowflake['y'] + snowflake['size']

                if (pacman_left < snowflake_right and pacman_right > snowflake_left and
                        pacman_top < snowflake_bottom and pacman_bottom > snowflake_top):
                    snowflake['eaten'] = True
                    points = 10
                    if snowflake.get('type') == 'crystal':
                        points = 15
                    elif snowflake.get('type') == 'star':
                        points = 20

                    self.players[player_id]['score'] += points
                    logger.info(f"❄️ Пакмен собрал снежинку! +{points} очков")

    async def check_icicle_collision(self, player_id: str, x: int, y: int):
        """Проверка столкновения с сосулькой"""
        pacman_left, pacman_top = x - 15, y - 15
        pacman_right, pacman_bottom = x + 15, y + 15

        for icicle in self.power_pellets:
            if not icicle['eaten']:
                icicle_left = icicle['x'] - 5
                icicle_right = icicle['x'] + 5
                icicle_top = icicle['y'] - 5
                icicle_bottom = icicle['y'] + 5

                if (pacman_left < icicle_right and pacman_right > icicle_left and
                        pacman_top < icicle_bottom and pacman_bottom > icicle_top):
                    icicle['eaten'] = True
                    self.players[player_id]['power_mode'] = True
                    self.players[player_id]['power_timer'] = 300
                    logger.info(f"🧊 Пакмен активировал ледяную силу!")

    async def check_ghost_collision(self, player_id: str, x: int, y: int):
        """Проверка столкновения с призраками"""
        pacman_left, pacman_top = x - 15, y - 15
        pacman_right, pacman_bottom = x + 15, y + 15

        power_mode = self.players[player_id]['power_mode']

        for ghost_id, ghost_data in self.players.items():
            if ghost_id != player_id and ghost_data['role'] == 'ghost':
                ghost_left = ghost_data['x'] - 15
                ghost_right = ghost_data['x'] + 15
                ghost_top = ghost_data['y'] - 15
                ghost_bottom = ghost_data['y'] + 15

                if (pacman_left < ghost_right and pacman_right > ghost_left and
                        pacman_top < ghost_bottom and pacman_bottom > ghost_top):

                    if power_mode:
                        # Пакмен замораживает призрака
                        logger.info(f"❄️ Пакмен заморозил призрака {ghost_id}!")
                        current_map = self.maps[self.current_map]
                        spawn_pos = random.choice(current_map['ghost_spawns'])
                        self.players[ghost_id]['x'] = spawn_pos[0]
                        self.players[ghost_id]['y'] = spawn_pos[1]
                        self.players[player_id]['score'] += 200
                    else:
                        # Призрак ловит Пакмена
                        logger.info(f"👻 Призрак {ghost_id} поймал Пакмена!")
                        self.players[player_id]['lives'] -= 1

                        if self.players[player_id]['lives'] <= 0:
                            # Пакмен умер - ищем нового
                            logger.info(f"💀 Пакмен замерз! Ищем нового игрока...")
                            old_pacman = player_id
                            self.pacman_player_id = None

                            # Сохраняем статистику
                            if 'name' in self.players[old_pacman]:
                                username = self.players[old_pacman]['name']
                                score = self.players[old_pacman]['score']
                                self.db.update_player_rating(username, score, False)

                            self.assign_roles()

                            # Телепортируем бывшего Пакмена как призрака
                            if old_pacman in self.players:
                                current_map = self.maps[self.current_map]
                                spawn_pos = random.choice(current_map['ghost_spawns'])
                                self.players[old_pacman]['x'] = spawn_pos[0]
                                self.players[old_pacman]['y'] = spawn_pos[1]
                                self.players[old_pacman]['color'] = self.get_ghost_color()
                        else:
                            # Возрождаем Пакмена в центре
                            current_map = self.maps[self.current_map]
                            self.players[player_id]['x'] = current_map['pacman_spawn'][0]
                            self.players[player_id]['y'] = current_map['pacman_spawn'][1]
                            logger.info(f"❤️ Пакмен отогрелся! Осталось жизней: {self.players[player_id]['lives']}")

    async def send_game_state(self, player_id: str):
        """Отправка состояния игры конкретному игроку"""
        if player_id not in self.players:
            return

        game_state = await self.prepare_game_state(player_id)
        try:
            await self.players[player_id]['websocket'].send(json.dumps(game_state))
        except:
            pass

    async def broadcast_game_state(self):
        """Рассылка состояния игры всем игрокам"""
        # Проверяем восстановление снежинок
        await self.check_snowflake_respawn()

        tasks = []
        for player_id in self.players.keys():
            game_state = await self.prepare_game_state(player_id)
            tasks.append(
                self.players[player_id]['websocket'].send(json.dumps(game_state))
            )

        # Игнорируем ошибки отправки
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                pass

    async def prepare_game_state(self, player_id: str) -> Dict:
        """Подготовка состояния игры для игрока"""
        players_data = {}
        for pid, pdata in self.players.items():
            players_data[pid] = {
                'x': pdata['x'],
                'y': pdata['y'],
                'role': pdata['role'],
                'color': pdata['color'],
                'score': pdata.get('score', 0),
                'power_mode': pdata.get('power_mode', False),
                'lives': pdata.get('lives', 3),
                'name': pdata.get('name', f'Player{pid}'),
                'voice_chat': pdata.get('voice_chat', False),
                'muted': pdata.get('muted', False)
            }

        return {
            'type': 'game_state',
            'players': players_data,
            'dots': [dot for dot in self.dots if not dot.get('eaten', False)],
            'power_pellets': [pellet for pellet in self.power_pellets if not pellet['eaten']],
            'walls': self.walls,
            'snowflakes': self.snowflakes,
            'current_map': self.current_map,
            'map_name': self.maps[self.current_map]['name'],
            'map_theme': self.maps[self.current_map]['background'],
            'your_role': self.players[player_id]['role'],
            'your_id': player_id,
            'pacman_id': self.pacman_player_id,
            'season': 'winter',
            'time_of_day': 'night'
        }

    async def cleanup_player(self, player_id: str):
        """Очистка данных игрока при отключении"""
        if player_id in self.players:
            # Сохраняем статистику если игрок был Пакменом
            if player_id == self.pacman_player_id and 'name' in self.players[player_id]:
                username = self.players[player_id]['name']
                score = self.players[player_id]['score']
                self.db.update_player_rating(username, score, True)
                logger.info(f"🏆 Сохранена статистика для {username}: {score} очков")

            # Освобождаем цвет призрака
            if self.players[player_id]['role'] == 'ghost':
                color = tuple(self.players[player_id]['color'])
                if color in self.used_ghost_colors:
                    self.used_ghost_colors.remove(color)

            # Если отключился Пакмен, назначаем нового
            if player_id == self.pacman_player_id:
                self.pacman_player_id = None
                logger.info(f"⚡ Пакмен отключился! Ищем нового...")

            # Удаляем игрока
            if self.players[player_id]['websocket'] in self.connected_clients:
                self.connected_clients.remove(self.players[player_id]['websocket'])
            del self.players[player_id]

            # Перераспределяем роли
            self.assign_roles()

    async def run_server(self):
        """Запуск WebSocket сервера"""
        logger.info("🔄 Запуск Winter Pacman WebSocket сервера...")

        server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=40
        )

        logger.info(f"✅ Сервер запущен на ws://{self.host}:{self.port}")
        logger.info("⏹️  Для остановки нажмите Ctrl+C")

        await asyncio.Future()


def main():
    """Основная функция"""
    print("🎮 Запуск Winter Pacman MultiPlayer Server...")

    server = WebSocketPacmanServer()

    try:
        asyncio.run(server.run_server())
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
    except Exception as e:
        print(f"❌ Критическая ошибка сервера: {e}")


if __name__ == "__main__":
    main()