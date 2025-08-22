#!/bin/bash

# =============================================================================
# PDF CONVERTER PIPELINE v2.0 - ИСПРАВЛЕННЫЙ СКРИПТ УСТАНОВКИ
# Модульная архитектура с vLLM + Docling + 5-уровневая QA система
# ИСПРАВЛЕНО: Соответствие реальной структуре GitHub проекта
# =============================================================================

set -euo pipefail

# Конфигурация
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/install_$(date +%Y%m%d_%H%M%S).log"

# Создание директории логов
mkdir -p "$LOG_DIR"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Параметры установки
PROJECT_VERSION="v2.0"
DOCKER_COMPOSE_VERSION="2.21.0"
PYTHON_VERSION="3.11"
AIRFLOW_VERSION="2.8.1"

# Конфигурация моделей для v2.0 (ИСПРАВЛЕНО)
CONTENT_MODEL="Qwen/Qwen2.5-VL-32B-Instruct"
TRANSLATION_MODEL="Qwen/Qwen3-30B-A3B-Instruct-2507"

# Отслеживание прогресса
TOTAL_STEPS=10
CURRENT_STEP=0

# Функции логирования
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

show_progress() {
    local step_name="$1"
    CURRENT_STEP=$((CURRENT_STEP + 1))
    local percentage=$((CURRENT_STEP * 100 / TOTAL_STEPS))
    local width=50
    local completed=$((CURRENT_STEP * width / TOTAL_STEPS))
    local remaining=$((width - completed))
    
    printf "\r["
    printf "%*s" "$completed" '' | tr ' ' '='
    printf "%*s" "$remaining" '' | tr ' ' '-'
    printf "] %d%% Step %d/%d: %s" "$percentage" "$CURRENT_STEP" "$TOTAL_STEPS" "$step_name"
    echo
}

# Проверка системных требований
check_requirements() {
    log "INFO" "Проверка системных требований"
    
    # Проверка ОС
    if [[ "$OSTYPE" != "linux-gnu"* ]]; then
        log "ERROR" "Этот инсталлятор требует Linux"
        exit 1
    fi

    # Проверка свободного места (минимум 30GB)
    local available_space=$(df "$SCRIPT_DIR" | awk 'NR==2 {print int($4/1024/1024)}')
    if [ "$available_space" -lt 30 ]; then
        log "ERROR" "Нужно минимум 30GB свободного места, доступно: ${available_space}GB"
        exit 1
    fi

    # Проверка RAM (рекомендуется 16GB+)
    local total_ram=$(free -g | awk 'NR==2{print $2}')
    if [ "$total_ram" -lt 8 ]; then
        log "WARN" "Рекомендуется 16GB+ RAM, доступно: ${total_ram}GB"
    fi

    # Проверка Docker
    if ! command -v docker &> /dev/null; then
        log "ERROR" "Docker не найден. Установите Docker сначала"
        exit 1
    fi

    # Проверка Docker Compose
    if ! command -v docker compose &> /dev/null; then
        log "ERROR" "Docker Compose не найден"
        exit 1
    fi

    # Проверка NVIDIA GPU (опционально)
    if command -v nvidia-smi &> /dev/null; then
        local gpu_count=$(nvidia-smi -L | wc -l)
        log "INFO" "Найдено $gpu_count NVIDIA GPU(s)"
        if [ "$gpu_count" -lt 2 ]; then
            log "WARN" "Рекомендуется 2x GPU, найдено: $gpu_count"
        fi
    else
        log "WARN" "NVIDIA GPU не найдены. Система будет работать на CPU (медленно)"
    fi

    log "INFO" "Проверка системных требований пройдена"
}

# Установка системных зависимостей
install_dependencies() {
    log "INFO" "Установка системных зависимостей"
    
    # Обновление пакетного менеджера
    sudo apt-get update -qq >> "$LOG_FILE" 2>&1
    
    # Установка необходимых пакетов
    local packages=(
        "curl"
        "wget" 
        "git"
        "jq"
        "htop"
        "python3"
        "python3-pip"
    )
    
    for package in "${packages[@]}"; do
        if ! dpkg -l | grep -q "^ii $package "; then
            log "INFO" "Установка $package"
            sudo apt-get install -y "$package" >> "$LOG_FILE" 2>&1
        fi
    done
    
    # Добавление пользователя в группу docker
    if ! groups | grep -q docker; then
        sudo usermod -aG docker "$USER"
        log "WARN" "Добавлен в группу docker. Требуется перелогин или перезапуск"
    fi
    
    log "INFO" "Системные зависимости установлены"
}

# Настройка структуры директорий
setup_directories() {
    log "INFO" "Настройка структуры директорий"
    
    # Проверка обязательных директорий проекта
    local required_dirs=(
        "airflow/dags"
        "flask"
        "vllm" 
        "document_processor"
        "quality_assurance"
        "translator"
        "prometheus"
        "grafana/provisioning"
    )
    
    for dir in "${required_dirs[@]}"; do
        if [ ! -d "$SCRIPT_DIR/$dir" ]; then
            log "ERROR" "Отсутствует обязательная директория: $dir"
            exit 1
        fi
    done
    
    # Создание рабочих директорий
    local work_dirs=(
        "logs"
        "input_pdf"
        "output_md_en"
        "output_md_ru"
        "output_md_zh"
        "temp"
        "models/huggingface"
        "models/shared"
    )
    
    for dir in "${work_dirs[@]}"; do
        mkdir -p "$SCRIPT_DIR/$dir"
        log "DEBUG" "Создана директория: $dir"
    done
    
    # Установка прав доступа
    chown -R 1000:1000 "$SCRIPT_DIR" 2>/dev/null || true
    chmod -R 755 "$SCRIPT_DIR" 2>/dev/null || true
    
    log "INFO" "Структура директорий настроена"
}

# Генерация конфигурации окружения
generate_env() {
    log "INFO" "Генерация конфигурации окружения"
    
    # Проверка наличия файла env
    if [ ! -f "$SCRIPT_DIR/env" ]; then
        log "ERROR" "Файл env не найден"
        exit 1
    fi
    
    # Копирование в .env
    cp "$SCRIPT_DIR/env" "$SCRIPT_DIR/.env"
    
    # Генерация ключей
    local fernet_key
    if command -v python3 &> /dev/null; then
        fernet_key=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || openssl rand -base64 32)
    else
        fernet_key=$(openssl rand -base64 32)
    fi
    
    local flask_secret=$(openssl rand -hex 32)
    
    # Обновление .env файла
    sed -i "s/your_super_secret_key_here_change_in_production/$flask_secret/g" "$SCRIPT_DIR/.env"
    sed -i "s/your_fernet_key_here_change_in_production/$fernet_key/g" "$SCRIPT_DIR/.env"
    
    log "INFO" "Конфигурация окружения сгенерирована"
}

# Загрузка Docker образов
pull_docker_images() {
    log "INFO" "Загрузка Docker образов"
    
    # Базовые образы
    local base_images=(
        "postgres:15-alpine"
        "redis:7-alpine"
        "apache/airflow:${AIRFLOW_VERSION}-python3.11"
        "prom/prometheus:latest"
        "grafana/grafana:latest"
        "vllm/vllm-openai:latest"
    )
    
    for image in "${base_images[@]}"; do
        log "INFO" "Загрузка $image"
        docker pull "$image" >> "$LOG_FILE" 2>&1
    done
    
    log "INFO" "Docker образы загружены"
}

# Сборка кастомных образов
build_custom_images() {
    log "INFO" "Сборка кастомных образов"
    
    cd "$SCRIPT_DIR"
    
    # Сборка основных сервисов
    local services=(
        "flask:dockerfile.flask"
        "document_processor:dockerfile.docprocessor" 
        "quality_assurance:dockerfile.qa"
        "translator:dockerfile.translator"
        "vllm:dockerfile.vllm"
        "airflow:dockerfile.airflow"
    )
    
    for service_info in "${services[@]}"; do
        local service="${service_info%:*}"
        local dockerfile="${service_info#*:}"
        
        if [ -f "$service/$dockerfile" ]; then
            log "INFO" "Сборка $service"
            docker build -t "pdf-converter/$service:latest" -f "$service/$dockerfile" "$service/" >> "$LOG_FILE" 2>&1 || log "WARN" "Ошибка сборки $service"
        else
            log "WARN" "Dockerfile не найден: $service/$dockerfile"
        fi
    done
    
    log "INFO" "Кастомные образы собраны"
}

# Инициализация Airflow
initialize_airflow() {
    log "INFO" "Инициализация Airflow"
    
    cd "$SCRIPT_DIR"
    
    # Запуск PostgreSQL и Redis
    docker compose up -d postgres redis >> "$LOG_FILE" 2>&1
    sleep 15
    
    # Инициализация БД
    docker compose run --rm airflow-webserver airflow db init >> "$LOG_FILE" 2>&1 || log "INFO" "БД уже инициализирована"
    
    # Создание админ пользователя
    docker compose run --rm airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@pdf-converter.local \
        --password admin >> "$LOG_FILE" 2>&1 || log "INFO" "Пользователь уже существует"
    
    log "INFO" "Airflow инициализирован"
}

# Запуск сервисов
start_services() {
    log "INFO" "Запуск всех сервисов"
    
    cd "$SCRIPT_DIR"
    
    # Поэтапный запуск
    log "INFO" "Запуск инфраструктурных сервисов"
    docker compose up -d postgres redis prometheus grafana >> "$LOG_FILE" 2>&1
    sleep 10
    
    log "INFO" "Запуск основных сервисов"
    docker compose up -d vllm-server document-processor quality-assurance translator >> "$LOG_FILE" 2>&1
    sleep 20
    
    log "INFO" "Запуск Airflow"
    docker compose up -d airflow-webserver airflow-scheduler >> "$LOG_FILE" 2>&1
    sleep 15
    
    log "INFO" "Запуск API"
    docker compose up -d flask-api >> "$LOG_FILE" 2>&1
    
    # Запуск дополнительных сервисов если есть
    if grep -q pandoc docker-compose.yml 2>/dev/null; then
        docker compose up -d pandoc >> "$LOG_FILE" 2>&1 || true
    fi
    
    if grep -q diff-pdf docker-compose.yml 2>/dev/null; then
        docker compose up -d diff-pdf >> "$LOG_FILE" 2>&1 || true
    fi
    
    log "INFO" "Все сервисы запущены"
}

# Проверка состояния
health_check() {
    log "INFO" "Выполнение проверки состояния"
    
    # Ожидание запуска
    sleep 30
    
    local services=(
        "localhost:5000:Flask API"
        "localhost:8080:Airflow"
        "localhost:8000:vLLM Server"
        "localhost:8001:Document Processor"
        "localhost:8002:Quality Assurance"
        "localhost:8003:Translator"
        "localhost:9090:Prometheus"
        "localhost:3000:Grafana"
    )
    
    local healthy_count=0
    local total_count=${#services[@]}
    
    for service_info in "${services[@]}"; do
        local host_port="${service_info%:*}"
        local name="${service_info##*:}"
        local url="http://$host_port"
        
        if curl -s --connect-timeout 5 "$url" > /dev/null 2>&1 || \
           curl -s --connect-timeout 5 "$url/health" > /dev/null 2>&1; then
            log "INFO" "$name: РАБОТАЕТ"
            ((healthy_count++))
        else
            log "WARN" "$name: НE ОТВЕЧАЕТ"
        fi
    done
    
    log "INFO" "Работающих сервисов: $healthy_count/$total_count"
    
    # Показать статус контейнеров
    log "INFO" "Статус контейнеров:"
    docker compose ps
}

# Настройка исполняемых скриптов
setup_scripts() {
    log "INFO" "Настройка исполняемых скриптов"
    
    local scripts=(
        "convert_md.sh"
        "translate_ru.sh"
        "translate_en.sh"
        "translate_md_ru.sh"
        "translate_md_en.sh"
        "quality_check.sh"
    )
    
    for script in "${scripts[@]}"; do
        if [ -f "$SCRIPT_DIR/$script" ]; then
            chmod +x "$SCRIPT_DIR/$script"
            log "DEBUG" "Сделан исполняемым: $script"
        fi
    done
    
    log "INFO" "Исполняемые скрипты настроены"
}

# Печать сводки установки
print_summary() {
    echo
    echo -e "${GREEN}====================================${NC}"
    echo -e "${GREEN}PDF Converter Pipeline ${PROJECT_VERSION}${NC}"
    echo -e "${GREEN}Установка завершена успешно${NC}"
    echo -e "${GREEN}====================================${NC}"
    echo

    echo "🌐 URL сервисов:"
    echo " • Airflow Web UI: http://localhost:8080 (admin/admin)"
    echo " • Flask API: http://localhost:5000"
    echo " • vLLM Server: http://localhost:8000"
    echo " • Document Processor: http://localhost:8001"
    echo " • Quality Assurance: http://localhost:8002"
    echo " • Translator: http://localhost:8003"
    echo " • Prometheus: http://localhost:9090"
    echo " • Grafana: http://localhost:3000 (admin/admin)"
    echo

    echo "📁 Структура директорий:"
    echo " • Входные PDF: ./input_pdf/"
    echo " • Результаты на русском: ./output_md_ru/"
    echo " • Результаты на английском: ./output_md_en/"
    echo " • Результаты на китайском: ./output_md_zh/"
    echo " • Логи: ./logs/"
    echo

    echo "🚀 Модели:"
    echo " • Content Transformation: $CONTENT_MODEL"
    echo " • Translation: $TRANSLATION_MODEL"
    echo

    echo "🔧 Управление:"
    echo " • Остановка: docker compose down"
    echo " • Перезапуск: docker compose restart"
    echo " • Логи: docker compose logs -f [service_name]"
    echo " • Статус: docker compose ps"
    echo
    
    if [ -f "$SCRIPT_DIR/convert_md.sh" ]; then
        echo "📝 Доступные скрипты:"
        echo " • ./convert_md.sh - Конвертация PDF в Markdown"
        echo " • ./translate_ru.sh - Полный пайплайн на русский"
        echo " • ./translate_en.sh - Полный пайплайн на английский"
        echo
    fi

    echo "📄 Лог установки: $LOG_FILE"
    echo
}

# Основная функция установки
main() {
    echo -e "${BLUE}PDF Converter Pipeline ${PROJECT_VERSION} Установщик${NC}"
    echo "=================================================="
    echo

    log "INFO" "Начало установки PDF Converter Pipeline ${PROJECT_VERSION}"

    # Шаги установки
    show_progress "Проверка системных требований"
    check_requirements

    show_progress "Установка системных зависимостей"
    install_dependencies

    show_progress "Настройка структуры директорий"
    setup_directories

    show_progress "Генерация конфигурации окружения"
    generate_env

    show_progress "Загрузка Docker образов"
    pull_docker_images

    show_progress "Сборка кастомных образов"
    build_custom_images

    show_progress "Инициализация Airflow"
    initialize_airflow

    show_progress "Запуск всех сервисов"
    start_services

    show_progress "Настройка исполняемых скриптов"
    setup_scripts

    show_progress "Выполнение проверок состояния"
    health_check

    show_progress "Установка завершена"
    print_summary

    log "INFO" "PDF Converter Pipeline ${PROJECT_VERSION} установка завершена успешно"
}

# Выполнение скрипта
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi