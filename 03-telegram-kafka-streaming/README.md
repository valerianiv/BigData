# Telegram → Kafka Streaming

Пайплайн, который в реальном времени читает новостные Telegram-каналы, извлекает имена собственные и считает их частоты в минутных окнах через Apache Kafka.

## Архитектура

```
Telegram-каналы ──► Telethon (producer) ──► Kafka topic ──► consumer ──► output/word_counts_minute_NN.txt
 rian_ru, tass,       извлечение и стемминг   telegram-messages  окна по 60 с     + топ-10 за 30 минут
 rbc_news, ...        имён собственных
```

- **Producer** (asyncio + Telethon) подписывается на 7 каналов, на каждое новое сообщение извлекает имена собственные и отправляет каждое отдельным событием `{word, timestamp, channel}` в Kafka (`acks=all`, ретраи).
- **Consumer** работает в отдельном потоке (`asyncio.to_thread`), читает топик через `poll` и раз в минуту сбрасывает частоты за окно в файл. Через 30 минут печатает итоговый топ-10.

### Извлечение имён собственных

Эвристика без тяжёлых NLP-моделей:

1. Текст делится на предложения по `.!?`.
2. Именем собственным считается слово с заглавной буквы **не в начале предложения**, которого нет в списке стоп-слов.
3. Простой стемминг: с конца отрезаются гласные и «й», поэтому «Москва», «Москвы» и «Москве» считаются одним словом.

## Запуск

1. Kafka (одиночный брокер в режиме KRaft, без ZooKeeper):

   ```bash
   docker compose up -d
   ```

2. Ключи Telegram API: [my.telegram.org](https://my.telegram.org) → API development tools.

   ```bash
   cp .env.example .env     # и заполнить TG_API_ID / TG_API_HASH
   set -a; source .env; set +a
   ```

3. Список стоп-слов, например [stopwords-iso/stopwords-ru](https://github.com/stopwords-iso/stopwords-ru):

   ```bash
   curl -L -o stop-words-russian.txt https://raw.githubusercontent.com/stopwords-iso/stopwords-ru/master/stopwords-ru.txt
   ```

4. Запуск (при первом входе Telethon спросит номер телефона и код):

   ```bash
   python src/proper_nouns_stream.py
   ```

Результат: файлы `output/word_counts_minute_00.txt` … `_29.txt` в формате `слово, количество`, отсортированные по убыванию, и топ-10 в консоли. `Ctrl+C` останавливает сбор досрочно и печатает текущий топ.

## Что можно улучшить

- Заменить эвристику на NER (Natasha или spaCy `ru_core_news_sm`).
- Перенести оконную агрегацию в Spark Structured Streaming или Kafka Streams.
- Хранить счётчики в Redis или ClickHouse вместо файлов.
