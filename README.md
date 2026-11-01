# Apache Kafka Learning Lab

Практические лабораторные работы по системному изучению Apache Kafka: от основ сетевого взаимодействия и распределённых систем до разработки приложений с Kafka.

## Цель

Изучить Apache Kafka на уровне, позволяющем самостоятельно проектировать, разрабатывать, тестировать и поддерживать event-driven системы.

## Технологии

- Python 3.12
- Docker
- Docker Compose
- Git
- Apache Kafka (будет добавлена в следующих модулях)

## Структура проекта

- `race_condition.py` — демонстрация состояния гонки.
- `race_condition_fixed.py` — исправление с помощью Lock.
- `process_memory.py` — изоляция памяти процессов.
- `worker_failure.py` — моделирование отказа процесса.
- `sync_async_demo.py` — синхронное и асинхронное выполнение.
- `docker-compose-lab/` — взаимодействие API и Worker через Docker Compose.
- `serialization-lab/` — JSON-сериализация и проверка контрактов событий.

## Запуск

Для Python-экспериментов:

```bash
python race_condition.py
```

Для Docker-лаборатории:

```bash
cd docker-compose-lab
docker compose up -d --build
```

## Прогресс

- [x] Клиент-серверная архитектура
- [x] Синхронное и асинхронное взаимодействие
- [x] Параллелизм и распределённые системы
- [x] Docker и Docker Compose
- [ ] JSON, сериализация и Git
- [ ] Apache Kafka Fundamentals

## Дальнейшее развитие

В следующих модулях будут добавлены Kafka Brokers, Producers, Consumers, Consumer Groups, механизмы репликации и обработки отказов.
