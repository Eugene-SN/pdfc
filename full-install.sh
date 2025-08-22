#!/bin/bash
# PDF Converter Pipeline v2.0 - Complete Installation Script (ИСПРАВЛЕН)

set -e  # Выйти при любой ошибке

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функции для логирования
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка root прав
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "Скрипт должен запускаться с правами root"
        exit 1
    fi
}

# Проверка системных требований
check_requirements() {
    log_info "Проверка системных требований..."
    
    # Проверка Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker не установлен"
        exit 1
    fi
    
    # Проверка Docker Compose
    if ! command -v docker compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose не установлен"
        exit 1
    fi
    
    # Проверка NVIDIA GPU
    if nvidia-smi &> /dev/null; then
        log_success "NVIDIA GPU обнаружен"
        nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits | while read gpu; do
            echo "$gpu"
        done
    else
        log_warning "NVIDIA GPU не обнаружен или nvidia-smi недоступен"
    fi
    
    log_success "Проверка системных требований завершена"
}

# Проверка файлов проекта
check_project_files() {
    log_info "Проверка файлов проекта..."
    
    local required_files=(
        "docker-compose.yml"
        ".env"
        "README.md"
    )
    
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
    
    for file in "${required_files[@]}"; do
        if [ -f "$file" ]; then
            log_success "Найден: $file"
        else
            log_error "Не найден файл: $file"
            exit 1
        fi
    done
    
    for dir in "${required_dirs[@]}"; do
        if [ -d "$dir" ]; then
            log_success "Найдена: $dir/"
        else
            log_error "Не найдена директория: $dir/"
            exit 1
        fi
    done
    
    log_success "Все необходимые файлы проекта найдены"
}

# Создание рабочих директорий
create_directories() {
    log_info "Создание рабочих директорий..."
    
    local dirs=(
        "input_pdf"
        "output_md_zh"
        "output_md_ru"
        "output_md_en"
        "temp"
        "logs"
        "models/huggingface"
        "models/shared"
        "airflow/config"
        "airflow/operators"
        "airflow/utils"
    )
    
    for dir in "${dirs[@]}"; do
        if mkdir -p "$dir" 2>/dev/null; then
            log_success "Создана: $dir/"
        else
            log_warning "Не удалось создать: $dir/"
        fi
    done
    
    # Установка правильных прав доступа
    chown -R 1000:1000 input_pdf output_md_* temp logs airflow/ 2>/dev/null || true
    
    # Создание пустых __init__.py файлов для Python модулей
    touch airflow/{config,operators,utils}/__init__.py
    
    log_success "Рабочие директории созданы"
}

# Генерация секретных ключей
generate_secrets() {
    log_info "Генерация секретных ключей..."
    
    # Проверяем наличие Python для генерации ключей
    if command -v python3 &> /dev/null; then
        # Генерация Fernet ключа
        FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null)
        if [ ! -z "$FERNET_KEY" ]; then
            # Обновление .env файла
            if [ -f ".env" ]; then
                sed -i "s/AIRFLOW__CORE__FERNET_KEY=.*/AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY/" .env
                log_success "Fernet ключ обновлен в .env файле"
            fi
        fi
    fi
    
    log_success "Секретные ключи сгенерированы и настроены"
}

# Создание внешней сети
create_network() {
    log_info "Создание внешней сети Docker..."
    
    if ! docker network ls | grep -q "ai-net"; then
        docker network create ai-net
        log_success "Сеть ai-net создана"
    else
        log_success "Сеть ai-net уже существует"
    fi
}

# Применение исправлений
apply_fixes() {
    log_info "Применение исправлений..."
    
    # Замена requirements файлов (если найдены исправленные версии)
    if [ -f "requirements-qa-fixed.txt" ]; then
        cp requirements-qa-fixed.txt quality_assurance/requirements-qa.txt
        log_success "Обновлен requirements-qa.txt"
    fi
    
    if [ -f "dockerfile-docprocessor-fixed" ]; then
        cp dockerfile-docprocessor-fixed document_processor/dockerfile.docprocessor  
        log_success "Обновлен dockerfile.docprocessor"
    fi
    
    if [ -f "docker-compose-fixed.yml" ]; then
        cp docker-compose-fixed.yml docker-compose.yml
        log_success "Обновлен docker-compose.yml"
    fi
    
    if [ -f "env-fixed" ]; then
        cp env-fixed .env
        log_success "Обновлен .env"
    fi
    
    log_success "Исправления применены"
}

# Загрузка базовых образов
pull_base_images() {
    log_info "Загрузка базовых образов..."
    
    local base_images=(
        "postgres:15-alpine"
        "redis:7-alpine"
        "apache/airflow:2.8.1-python3.11"
        "prom/prometheus:latest"
        "grafana/grafana:latest"
        "vllm/vllm-openai:latest"
    )
    
    for image in "${base_images[@]}"; do
        docker pull "$image"
    done
    
    log_success "Базовые образы загружены"
}

# Сборка кастомных образов
build_custom_images() {
    log_info "Сборка кастомных образов..."
    
    # Flask API
    log_info "Сборка Flask API..."
    if docker build -t pdf-converter/flask-api:latest -f flask/dockerfile.flask ./flask/; then
        log_success "Flask API собран"
    else
        log_warning "Ошибка сборки Flask API"
    fi
    
    # Document Processor
    log_info "Сборка Document Processor..."
    if docker build -t pdf-converter/document-processor:latest -f document_processor/dockerfile.docprocessor ./document_processor/; then
        log_success "Document Processor собран"
    else
        log_warning "Ошибка сборки Document Processor"
    fi
    
    # Quality Assurance
    log_info "Сборка Quality Assurance..."
    if docker build -t pdf-converter/quality-assurance:latest -f quality_assurance/dockerfile.qa ./quality_assurance/; then
        log_success "Quality Assurance собран"
    else
        log_warning "Ошибка сборки QA"
    fi
    
    # Translator
    log_info "Сборка Translator..."
    if docker build -t pdf-converter/translator:latest -f translator/dockerfile.translator ./translator/; then
        log_success "Translator собран"
    else
        log_warning "Ошибка сборки Translator"
    fi
    
    # vLLM Server
    log_info "Сборка vLLM Server..."
    if docker build -t pdf-converter/vllm:latest -f vllm/dockerfile.vllm ./vllm/; then
        log_success "vLLM Server собран"
    else
        log_warning "Ошибка сборки vLLM"
    fi
    
    # Airflow
    log_info "Сборка Airflow..."
    if docker build -t pdf-converter/airflow:latest -f airflow/dockerfile.airflow ./airflow/; then
        log_success "Airflow собран"
    else
        log_warning "Ошибка сборки Airflow"
    fi
    
    log_success "Docker образы собраны"
}

# Инициализация Airflow
init_airflow() {
    log_info "Инициализация Airflow..."
    
    # Проверяем, инициализирована ли уже БД
    if docker compose ps postgres | grep -q "Up"; then
        log_warning "Airflow DB уже инициализирована"
    else
        # Запуск postgres и redis для инициализации
        docker compose up -d postgres redis
        sleep 10
        
        # Инициализация БД Airflow
        docker compose run --rm airflow-init
        
        log_success "Airflow инициализирован"
    fi
}

# Запуск системы
start_system() {
    log_info "Запуск PDF Converter Pipeline..."
    
    # Запуск инфраструктурных сервисов
    log_info "Запуск инфраструктурных сервисов..."
    docker compose up -d postgres redis prometheus grafana
    
    # Ожидание готовности БД
    sleep 15
    
    # Запуск основных сервисов
    log_info "Запуск основных сервисов..."
    docker compose up -d
    
    log_success "PDF Converter Pipeline запущен!"
    
    echo ""
    echo "=== СИСТЕМА ГОТОВА К РАБОТЕ ==="
    echo ""
    echo "Доступные сервисы:"
    echo "- Airflow UI: http://localhost:8090 (admin/admin)"
    echo "- Flask API: http://localhost:5000"
    echo "- vLLM API: http://localhost:8000/docs"
    echo "- Grafana: http://localhost:3000 (admin/admin)"
    echo "- Prometheus: http://localhost:9090"
    echo ""
    echo "Для просмотра логов: docker compose logs -f [service-name]"
    echo "Для остановки: docker compose down"
    echo ""
}

# Основная функция
main() {
    log_info "Начало установки PDF Converter Pipeline v2.0..."
    
    check_root
    check_requirements
    check_project_files
    create_directories
    generate_secrets
    apply_fixes
    create_network
    
    log_info "Сборка Docker образов..."
    pull_base_images
    build_custom_images
    
    init_airflow
    start_system
    
    log_success "Установка PDF Converter Pipeline v2.0 завершена!"
}

# Запуск основной функции
main "$@"