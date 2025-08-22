# PDF Converter Pipeline v2.0 - Модульная Архитектура

## 🚀 Обзор системы

**PDF Converter Pipeline v2.0** - это высокопроизводительная система для конвертации и перевода PDF документов с применением модульной архитектуры, искусственного интеллекта и 5-уровневой системы валидации качества.

### ⭐ Ключевые особенности

- 🧠 **vLLM Integration**: Замена Ollama на высокопроизводительный vLLM для ускорения обработки
- 🏗️ **Модульная архитектура**: 4 специализированных DAG + Master Orchestrator
- 📄 **Docling + OCR**: Точное извлечение контента с IBM Docling + PaddleOCR + Tesseract
- 🔍 **5-уровневая QA система**: Достижение 100% качества через многоуровневую валидацию
- 🌐 **Многоязычность**: Поддержка китайского, английского и русского языков
- 📊 **Полный мониторинг**: Prometheus + Grafana с детальными дашбордами
- ⚡ **GPU оптимизация**: Поддержка 2x NVIDIA A6000 с CUDA 12.9

## 🏛️ Архитектура системы

```
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ Flask API       │ │ Master DAG      │ │ Monitoring      │
│ (Gateway)       │◄──►│ (Orchestrator)  │◄──►│ Prometheus+     │
└─────────────────┘ └─────────────────┘ │ Grafana         │
                 │                     └─────────────────┘
                 ▼
┌───────────────────────────────────────────────────────┐
│ Modular DAG Pipeline                                  │
└───────────────────────────────────────────────────────┘
                 │
┌───────────────────────┼───────────────────────────────┐
▼                       ▼                               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ DAG 1        │ │ DAG 2        │ │ DAG 3        │ │ DAG 4        │
│ Document     │───►│ Content      │───►│ Translation  │───►│ Quality      │
│ Preprocessing│ │Transformation│ │ Pipeline     │ │ Assurance    │
│              │ │              │ │              │ │ (5 levels)   │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
       │                │                │                │
       ▼                ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Docling +    │ │ vLLM         │ │ vLLM         │ │ OCR Cross-   │
│ PaddleOCR +  │ │ Qwen2.5-VL   │ │ Qwen3-30B-   │ │ Validation + │
│ Tesseract +  │ │ Markdown     │ │ A3B-Instruct │ │ Visual Diff +│
│ Tabula-Py    │ │ Generation   │ │ Translation  │ │ AST Compare +│
│              │ │              │ │ Tech Terms   │ │ Auto-Correct │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

## 🔄 Модульные DAG

### DAG 1: Document Preprocessing
- **Цель**: Чистое извлечение контента без потерь
- **Компоненты**: IBM Docling, PaddleOCR + Tesseract кросс-валидация, Tabula-Py для таблиц
- **Выходы**: Структурированный контент, метаданные документа

### DAG 2: Content Transformation
- **Цель**: Преобразование в высококачественный Markdown
- **Компоненты**: vLLM с Qwen2.5-VL-32B-Instruct, интеллектуальная постобработка
- **Выходы**: Структурированный Markdown с сохранением технических терминов

### DAG 3: Translation Pipeline
- **Цель**: Высококачественный перевод с сохранением структуры
- **Компоненты**: vLLM с Qwen3-30B-A3B-Instruct-2507, специальные промпты для технических документов
- **Выходы**: Переведенный контент с сохранением команд IPMI/BMC/Redfish

### DAG 4: Quality Assurance (5 уровней)
- **Уровень 1**: OCR кросс-валидация (PaddleOCR + Tesseract)
- **Уровень 2**: Визуальное сравнение и SSIM анализ
- **Уровень 3**: AST сравнение структур документов
- **Уровень 4**: Валидация содержимого (таблицы, код, изображения, термины)
- **Уровень 5**: Автокоррекция и итоговая оценка (цель: 100%)

## 🛠️ Требования системы

### Минимальные требования
- **ОС**: Ubuntu 22.04 LTS или новее
- **RAM**: 32GB+ (рекомендуется 64GB)
- **Storage**: 500GB+ свободного места
- **GPU**: 2x NVIDIA A6000 (24GB VRAM каждая) или эквивалент
- **CUDA**: 12.9+
- **Docker**: 24.0+
- **Docker Compose**: 2.20+

### Рекомендуемые требования
- **RAM**: 128GB
- **Storage**: 1TB NVMe SSD
- **GPU**: 2x NVIDIA H100 или A100
- **Network**: 10Gbps для загрузки моделей

## 📦 Быстрая установка

### Автоматическая установка (рекомендуется)

```bash
# Клонирование проекта
git clone https://github.com/Eugene-SN/pdf-converter
cd pdf-converter

# Запуск полной автоматической установки
chmod +x full-install.sh
sudo ./full-install.sh
```

Скрипт автоматически:
- ✅ Проверит системные требования
- ✅ Создаст структуру проекта
- ✅ Разместит файлы по правильным местам
- ✅ Сгенерирует секретные ключи
- ✅ Загрузит модели vLLM
- ✅ Соберет Docker образы
- ✅ Запустит всю систему
- ✅ Проверит работоспособность

### Ручная установка

<details>
<summary>Развернуть инструкции по ручной установке</summary>

```bash
# 1. Создание структуры проекта
mkdir -p /mnt/storage/docker/pdf-converter
cd /mnt/storage/docker/pdf-converter

# 2. Создание директорий
mkdir -p flask airflow/dags vllm document_processor quality_assurance translator pandoc/templates diff-pdf
mkdir -p grafana/provisioning/{dashboards,datasources} prometheus
mkdir -p config logs plugins temp input_pdf output_md_{en,ru,zh} models/{huggingface,shared}

# 3. Размещение файлов (см. раздел "Структура файлов")
# Переместите все файлы согласно mapping в deployment-guide.md

# 4. Генерация секретных ключей
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
FLASK_SECRET=$(openssl rand -hex 32)

# 5. Обновление .env файла
sed -i "s/your_super_secret_key_here_change_in_production/$FLASK_SECRET/g" .env
sed -i "s/your_fernet_key_here_change_in_production/$FERNET_KEY/g" .env

# 6. Установка прав доступа
chown -R 1000:1000 /mnt/storage/docker/pdf-converter
chmod -R 755 /mnt/storage/docker/pdf-converter

# 7. Сборка и запуск
docker compose build
docker compose up -d
```
</details>

## 🌐 Веб-интерфейсы

После успешной установки доступны следующие интерфейсы:

| Сервис | URL | Логин/Пароль | Описание |
|--|--|--|--|
| **Flask API** | http://localhost:5000 | - | Главный API для загрузки и управления |
| **Airflow UI** | http://localhost:8080 | admin/admin | Управление DAG и мониторинг задач |
| **Grafana** | http://localhost:3000 | admin/admin | Дашборды мониторинга и метрики |
| **Prometheus** | http://localhost:9090 | - | Сбор метрик и алерты |

## 🔧 Использование системы

### Загрузка файлов через API

```bash
# Базовая конвертация
curl -X POST \
  -F "file=@document.pdf" \
  -F "target_language=en" \
  -F "quality_level=high" \
  http://localhost:5000/api/v1/convert

# Пакетная обработка
curl -X POST \
  -F "files=@document1.pdf" \
  -F "files=@document2.pdf" \
  -F "target_language=ru" \
  -F "batch_mode=true" \
  http://localhost:5000/api/v1/batch_convert
```

### Загрузка через веб-интерфейс

1. Откройте http://localhost:5000
2. Выберите PDF файлы для обработки
3. Укажите целевой язык (en/ru/zh/original)
4. Выберите уровень качества (high/medium/fast)
5. Нажмите "Конвертировать"
6. Отслеживайте прогресс в реальном времени

### Мониторинг обработки

- **Airflow UI**: Детальное отслеживание каждого DAG и задачи
- **Grafana**: Визуализация метрик производительности и качества
- **Логи**: `docker compose logs -f [service_name]`

## 📊 Мониторинг и метрики

### Ключевые метрики в Grafana

- **Pipeline Success Rate**: Процент успешно обработанных документов
- **Processing Time by DAG**: Время выполнения каждого модульного DAG
- **Quality Assurance Scores**: Баллы 5-уровневой валидации
- **vLLM Performance**: Метрики производительности AI моделей
- **System Resources**: Использование CPU, памяти и GPU

### Алерты и уведомления

Система автоматически отправляет уведомления при:
- Ошибках в любом из DAG
- Снижении качества ниже 95%
- Превышении времени обработки
- Проблемах с GPU или моделями

## 🔍 Устранение неполадок

### Часто встречающиеся проблемы

#### vLLM не запускается
```bash
# Проверка GPU
nvidia-smi

# Проверка CUDA
nvcc --version

# Логи vLLM
docker compose logs vllm
```

#### Airflow DAG не отображаются
```bash
# Проверка папки DAG
ls -la airflow/dags/

# Перезапуск Airflow
docker compose restart airflow-webserver airflow-scheduler
```

#### Проблемы с правами доступа
```bash
# Исправление прав
sudo chown -R 1000:1000 /mnt/storage/docker/pdf-converter
sudo chmod -R 755 /mnt/storage/docker/pdf-converter
```

#### Недостаточно памяти
```bash
# Мониторинг использования памяти
docker stats

# Освобождение памяти
docker system prune -f
```

## 🗂️ Структура выходных файлов

```
output_md_{язык}/
└── [timestamp]_[документ]/
    ├── document.md         # Основной файл
    ├── images/             # Извлеченные изображения
    │   ├── image_001.png
    │   └── image_002.png
    ├── metadata.json       # Метаданные обработки
    └── qa_report.json      # Отчет валидации качества
```

## 🔄 Управление системой

### Основные команды

```bash
# Статус всех сервисов
docker compose ps

# Остановка системы
docker compose down

# Перезапуск системы
docker compose restart

# Просмотр логов
docker compose logs -f [service_name]

# Обновление системы
git pull
docker compose build
docker compose up -d
```

### Масштабирование

```bash
# Увеличение количества воркеров
docker compose up -d --scale document-processor=3

# Масштабирование Quality Assurance
docker compose up -d --scale quality-assurance=2
```

## 🛡️ Безопасность

- 🔐 Все сервисы работают под непривилегированными пользователями (1000:1000)
- 🔑 Автоматическая генерация секретных ключей при установке
- 🚫 Отсутствие хранения чувствительных данных в репозитории
- 🛡️ Изоляция сервисов через Docker networks
- 📝 Детальное логирование всех операций

## 🚀 Производительность

### Ожидаемая производительность

| Метрика | Значение | Условия |
|--|--|--|
| **Пропускная способность** | 50-100 страниц/мин | 2x A6000, высокое качество |
| **Время обработки** | 2-5 мин/документ | Документ 10-20 страниц |
| **Качество OCR** | 95-99% | Четкие PDF документы |
| **Качество перевода** | 90-95% | Технические документы |
| **Общий балл QA** | 95-100% | После 5-уровневой валидации |

### Оптимизация производительности

- Используйте пакетную обработку для множественных файлов
- Настройте vLLM для ваших конкретных GPU
- Мониторьте метрики в Grafana для выявления узких мест
- Масштабируйте сервисы обработки по необходимости

## 📚 Дополнительная документация

- 📋 Подробная инструкция по развертыванию
- 🏗️ Архитектурное описание
- 🔧 API документация
- 📊 Руководство по мониторингу
- 🐛 Руководство по устранению неполадок

## 🤝 Вклад в проект

1. Fork проекта
2. Создайте feature branch (`git checkout -b feature/amazing-feature`)
3. Commit изменения (`git commit -m 'Add amazing feature'`)
4. Push в branch (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

## 📄 Лицензия

Этот проект лицензирован под MIT License - см. файл LICENSE для деталей.

## 🆘 Поддержка

- 📧 Email: support@pdf-converter.local
- 💬 Issues: GitHub Issues
- 📖 Wiki: Проектная Wiki

## 🎯 Roadmap

### v2.1 (Планируется)
- Поддержка дополнительных форматов (DOCX, PPTX)
- Интеграция с облачными хранилищами
- REST API v2 с расширенными возможностями
- Машинное обучение для автоулучшения качества

### v3.0 (Будущее)
- Веб-интерфейс для управления
- Поддержка дополнительных языков
- Кластерное развертывание
- Advanced AI модели для специализированных документов

**PDF Converter Pipeline v2.0** - мощное решение для высококачественной конвертации и перевода PDF документов с использованием современных технологий ИИ и модульной архитектуры. 🚀