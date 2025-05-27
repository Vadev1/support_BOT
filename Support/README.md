# Telegram Support Bot

Бот поддержки для Telegram с функциями управления диалогами, мониторинга и статистики.

## Возможности

- 👥 Двусторонняя коммуникация между клиентами и администраторами
- 📊 Статистика и мониторинг диалогов
- 🔒 Система ролей (администраторы 1-го и 2-го уровня)
- 📝 Логирование всех действий
- 💾 SQLite база данных для хранения диалогов и сообщений

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd support-bot
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Создайте файл `.env` на основе `.env.example`:
```bash
cp .env.example .env
```

4. Заполните `.env` файл:
```
TELEGRAM_BOT_TOKEN=your_bot_token_here
ADMIN_PASSWORD=your_admin_password_here
ADMIN_CHAT_ID=your_admin_chat_id_here
LEVEL2_ADMIN_ID=your_level2_admin_id_here
```

## Запуск

```bash
python support_bot.py
```

## Команды бота

- `/start` - Начало работы с ботом
- `/admin` - Доступ к админ-панели
- `/set_tag <пароль> <тег>` - Установка тега администратора
- `/set_level <user_id> <уровень>` - Установка уровня администратора (только для админов 2-го уровня)

## Структура базы данных

### Таблица admins
- id: INTEGER PRIMARY KEY
- user_id: INTEGER UNIQUE
- tag: TEXT
- level: INTEGER
- password: TEXT

### Таблица dialogues
- id: INTEGER PRIMARY KEY
- client_id: INTEGER
- admin_id: INTEGER
- status: TEXT
- start_time: TIMESTAMP
- end_time: TIMESTAMP

### Таблица messages
- id: INTEGER PRIMARY KEY
- dialogue_id: INTEGER
- user_id: INTEGER
- message: TEXT
- timestamp: TIMESTAMP

## Логирование

Логи сохраняются в файл `support_bot.log` и выводятся в консоль. Формат лога:
```
[время] [уровень] сообщение
```

## Безопасность

- Токен бота и пароль админа хранятся в переменных окружения
- Проверка уровней доступа для команд
- Безопасное хранение паролей в базе данных

## Масштабирование

Для больших нагрузок рекомендуется:
1. Перейти на PostgreSQL вместо SQLite
2. Добавить индексы в базу данных
3. Использовать систему ротации логов
4. Внедрить систему кэширования 