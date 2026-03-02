<h1 align="center">
  🔥 Telegram Multi-Account Manager
</h1>

<p align="center">
  <em>Менеджер множества Telegram-аккаунтов с бот‑панелью: подключение, рассылки, подписки, реакции, отчёты, статистика, прокси.</em>
</p>

<p align="center">
  <a href="https://t.me/dex2die">
    <img src="https://img.shields.io/badge/author-@rnxcode-red?style=for-the-badge&logo=telegram" alt="Author"/>
  </a>
  <img src="https://img.shields.io/badge/license-MIT-black?style=for-the-badge" alt="License"/>
  <img src="https://img.shields.io/badge/python-3.10+-white?style=for-the-badge&logo=python" alt="Python"/>
</p>

---

## 🚀 Возможности

| ![Broadcast](.assets/feature-broadcast.svg) | **Рассылки** — массовая отправка сообщений. |
|:--|:--|
| ![Subscribers](.assets/feature-subscribers.svg) | **Подписки** — массовое управление подписками аккаунтов. |
| ![Unsubscribe](.assets/feature-unsubscribe.svg) | **Отписки** — массовые отписки от каналов. |
| ![Analytics](.assets/feature-analytics.svg) | **Статистика** — метрики активности и доставляемости. |
| ![Status](.assets/feature-status.svg) | **Статус** — текущее состояние аккаунтов, лог операций. |
| ![Reactions](.assets/feature-reactions.svg) | **Реакции** — массовые реакции на посты. |
| ![Reports](.assets/feature-reports.svg) | **Репорты** — пакетные жалобы на цели. |
| ![Spam](.assets/feature-spam.svg) | **Спам** — массовая отправка в заданные цели. |
| ![Admin](.assets/feature-admin.svg) | **Админ‑панель** — контроль доступа и админ‑рассылки. |
| ![Proxy](.assets/feature-proxy.svg) | **Прокси‑пул** — автообновление и ротация прокси. |

---

## ✅ Требования

- Python 3.10+
- Доступ к Telegram API (API ID и API HASH)
- Токен бота для админ‑панели

---

## ⚙️ Установка

```bash
git clone https://github.com/dex2die/telegram-botnet.git
cd telegram-botnet
python -m venv .venv
source .venv/bin/activate  # или .venv\Scripts\activate на Windows
pip install -r requirements.txt
```

---

## 🔧 Настройка

Откройте `main.py` и замените значения в блоке конфигурации:

1. `API_ID` и `API_HASH`
2. `BOT_TOKEN`
3. `ADMIN_IDS`
4. `REQUIRED_CHANNEL` и `REQUIRED_CHAN_URL`
5. `ERROR_CHAT_ID`
6. `PROXY_SOURCES` при необходимости

---

## ▶️ Запуск

```bash
python main.py
```

---

## 📁 Важные файлы и данные

- `accounts.json` — подключённые аккаунты и session strings
- `subscriptions.json` — сохранённые списки для подписок
- `manager.db` — SQLite база пользователей, сессий и событий
- `sessions/` — локальные сессии Pyrogram
- `statistics/` — служебные метрики

---

## ♻️ Гайд по обновлению

1. Остановите процесс бота.
2. Сделайте резервную копию: `accounts.json`, `subscriptions.json`, `manager.db`, `sessions/`, `statistics/`.
3. Обновите код репозитория.

```bash
git fetch --all --prune
git pull
```

4. Обновите зависимости.

```bash
pip install -r requirements.txt
```

5. Проверьте блок конфигурации в `start.py` и перенесите свои значения, если они изменились.
6. Запустите проект.

```bash
python main.py
```

Если в будущих версиях появятся изменения схемы БД, это будет отражено в релизах и потребуется отдельный шаг миграции.

---

## 🔒 Безопасность

- Не публикуйте `BOT_TOKEN`, `API_HASH` и session strings.
- Держите резервные копии `manager.db` и `accounts.json` в безопасном месте.
