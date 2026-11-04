"""Потоковый подсчёт имён собственных в новостных Telegram-каналах через Kafka.

Producer: Telethon слушает каналы, извлекает имена собственные и пишет их в топик Kafka.
Consumer: читает топик, каждую минуту сохраняет частоты слов в файл,
через 30 минут печатает топ-10.

Ключи Telegram берутся из переменных окружения TG_API_ID и TG_API_HASH
(получить: https://my.telegram.org -> API development tools).
"""
import asyncio
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
CHANNELS = ["rian_ru", "bbcrussian", "meduzalive", "rt_russian", "lentach", "tass_agency", "rbc_news"]

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram-messages")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
STOPWORDS_PATH = os.getenv("STOPWORDS_PATH", "stop-words-russian.txt")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

WINDOW_SECONDS = 60
TOTAL_MINUTES = 30


def load_stopwords(path):
    try:
        with open(path, encoding="utf-8") as f:
            words = {line.strip().lower() for line in f if line.strip() and not line.startswith("#")}
        print(f"Загружено {len(words)} стоп-слов")
        return words
    except FileNotFoundError:
        print(f"Файл {path} не найден, стоп-слова не используются")
        return set()


STOPWORDS_RU = load_stopwords(STOPWORDS_PATH)


def is_proper_noun(word):
    return bool(re.fullmatch(r"[А-ЯЁ][а-яё]+", word)) and word.lower() not in STOPWORDS_RU


def stem_proper(word):
    # Простой стемминг: отрезаем гласные и «й» с конца, чтобы «Москва», «Москвы», «Москве» схлопнулись
    while len(word) > 1 and word[-1] in "ауоыиэяюёеий":
        word = word[:-1]
    return word


def extract_proper_nouns(text):
    """Слова с заглавной буквы не в начале предложения считаем именами собственными."""
    nouns = []
    for sentence in re.split(r"[.!?]", text or ""):
        words = re.findall(r"\b[А-Яа-яЁё]+\b", sentence)
        for word in words[1:]:
            if is_proper_noun(word):
                stemmed = stem_proper(word)
                if len(stemmed) >= 2:
                    nouns.append(stemmed)
    return nouns


class ProperNounCounter:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.word_counts = defaultdict(int)
        self.running = True

    def setup_kafka(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            retries=3,
            acks="all",
        )
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        print("Kafka producer и consumer запущены")

    async def start_telegram_client(self):
        client = TelegramClient("telegram_session", API_ID, API_HASH)
        await client.start()
        print("Telegram-клиент запущен")
        for channel in CHANNELS:
            try:
                await client(JoinChannelRequest(channel))
                print(f"Подписка на канал: {channel}")
            except Exception as e:
                print(f"Не удалось подписаться на {channel}: {e}")
        return client

    def save_window(self, counts, minute):
        OUTPUT_DIR.mkdir(exist_ok=True)
        path = OUTPUT_DIR / f"word_counts_minute_{minute:02d}.txt"
        with open(path, "w", encoding="utf-8") as f:
            for word, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
                f.write(f"{word}, {count}\n")
        print(f"Окно {minute} сохранено в {path}")

    def process_messages(self):
        start = time.time()
        for minute in range(TOTAL_MINUTES):
            if not self.running:
                break
            window_start = time.time()
            window_counts = defaultdict(int)
            while time.time() - window_start < WINDOW_SECONDS:
                try:
                    for records in self.consumer.poll(timeout_ms=1000).values():
                        for record in records:
                            word = record.value.get("word")
                            if word:
                                self.word_counts[word] += 1
                                window_counts[word] += 1
                except Exception as e:
                    print(f"Ошибка чтения из Kafka: {e}")
                    time.sleep(1)
            self.save_window(window_counts, minute)
            print(f"Минута {minute + 1}/{TOTAL_MINUTES}: {sum(window_counts.values())} слов")
            if time.time() - start >= TOTAL_MINUTES * WINDOW_SECONDS:
                break
        self.print_top10()

    def print_top10(self):
        top10 = sorted(self.word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        print("\n" + "=" * 50)
        print(f"Топ-10 имён собственных за {TOTAL_MINUTES} минут:")
        print("=" * 50)
        for i, (word, count) in enumerate(top10, 1):
            print(f"{i:2d}. {word:15} - {count:4d}")
        print("=" * 50)

    async def run_collector(self):
        client = await self.start_telegram_client()

        @client.on(events.NewMessage(chats=CHANNELS))
        async def handler(event):
            nouns = extract_proper_nouns(event.message.message)
            timestamp = (event.message.date or datetime.now()).isoformat()
            channel = getattr(event.chat, "username", "unknown")
            for noun in nouns:
                self.producer.send(KAFKA_TOPIC, {"word": noun, "timestamp": timestamp, "channel": channel})

        await client.run_until_disconnected()

    async def main(self):
        self.setup_kafka()
        try:
            await asyncio.gather(self.run_collector(), asyncio.to_thread(self.process_messages))
        finally:
            self.running = False
            self.producer.close()
            self.consumer.close()


if __name__ == "__main__":
    counter = ProperNounCounter()
    try:
        asyncio.run(counter.main())
    except KeyboardInterrupt:
        print("\nОстановлено досрочно. Текущий топ-10:")
        counter.print_top10()
