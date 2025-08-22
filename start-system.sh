#!/bin/bash

# ===============================================================================
# PDF CONVERTER PIPELINE v2.0 - ИСПРАВЛЕННЫЙ БЫСТРЫЙ ЗАПУСК
# Для системы, которая уже установлена
# ===============================================================================

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Проверка готовности системы
check_system_ready() {
  log_info "Проверка готовности системы..."
  
  # Проверка Docker
  if ! command -v docker &> /dev/null; then
    log_error "Docker не найден. Запустите сначала install.sh"
    exit 1
  fi
  
  # Проверка Docker Compose
  if ! command -v docker compose &> /dev/null; then
    log_error "Docker Compose не найден"
    exit 1
  fi
  
  # Проверка обязательных файлов
  if [ ! -f "$PROJECT_DIR/docker-compose.yml" ]; then
    log_error "docker-compose.yml не найден"
    exit 1
  fi
  
  if [ ! -f "$PROJECT_DIR/.env" ] && [ ! -f "$PROJECT_DIR/env" ]; then
    log_error "Файл конфигурации .env не найден"
    exit 1
  fi
  
  # Создание .env из env если нужно
  if [ ! -f "$PROJECT_DIR/.env" ] && [ -f "$PROJECT_DIR/env" ]; then
    cp "$PROJECT_DIR/env" "$PROJECT_DIR/.env"
    log_info "Создан .env файл из env"
  fi
  
  log_success "Система готова к запуску"
}

# Запуск сервисов
start_services() {
  log_info "Запуск PDF Converter Pipeline v2.0..."
  
  cd "$PROJECT_DIR"
  
  # Остановка существующих контейнеров
  docker compose down 2>/dev/null || true
  
  # Поэтапный запуск
  log_info "Шаг 1/4: Запуск базовых сервисов..."
  docker compose up -d postgres redis
  sleep 10
  
  log_info "Шаг 2/4: Запуск инфраструктурных сервисов..."
  docker compose up -d prometheus grafana statsd-exporter
  sleep 5
  
  log_info "Шаг 3/4: Запуск основных сервисов..."
  # ИСПРАВЛЕНО: правильное имя vLLM сервиса
  docker compose up -d dynamic-vllm-server document-processor quality-assurance translator
  sleep 20
  
  log_info "Шаг 4/4: Запуск Airflow и API..."
  docker compose up -d airflow-init
  sleep 10
  docker compose up -d airflow-webserver airflow-scheduler airflow-worker flask-api
  sleep 10
  
  # Запуск дополнительных сервисов если есть
  docker compose up -d pandoc-render diff-pdf 2>/dev/null || true
  
  log_success "Все сервисы запущены"
}

# Проверка здоровья сервисов
health_check() {
  log_info "Проверка работоспособности сервисов..."
  
  sleep 30 # Ожидание полного запуска
  
  local services=(
    "5000:Flask API"
    "8090:Airflow UI"  # ИСПРАВЛЕНО: правильный порт
    "8000:Dynamic vLLM Server"  # ИСПРАВЛЕНО: правильное имя
    "8001:Document Processor"
    "8002:Quality Assurance"
    "8003:Translator"
    "9090:Prometheus"
    "3000:Grafana"
  )
  
  local healthy=0
  local total=${#services[@]}
  
  for service in "${services[@]}"; do
    local port="${service%:*}"
    local name="${service#*:}"
    
    if curl -s --connect-timeout 3 "http://localhost:$port" > /dev/null 2>&1 || \
       curl -s --connect-timeout 3 "http://localhost:$port/health" > /dev/null 2>&1; then
      log_success "$name (порт $port) ✅"
      ((healthy++))
    else
      log_warning "$name (порт $port) ❌"
    fi
  done
  
  log_info "Работающих сервисов: $healthy/$total"
  
  if [ $healthy -ge $((total * 2 / 3)) ]; then
    log_success "Система успешно запущена!"
    return 0
  else
    log_warning "Некоторые сервисы не отвечают. Проверьте логи."
    return 1
  fi
}

# Показ информации о системе
show_info() {
  echo
  log_success "================================================================"
  log_success "PDF Converter Pipeline v2.0 ЗАПУЩЕН!"
  log_success "================================================================"
  echo
  
  echo "🌐 Веб-интерфейсы:"
  echo "  • Flask API: http://localhost:5000"
  echo "  • Airflow UI: http://localhost:8090 (admin/admin)"  # ИСПРАВЛЕНО
  echo "  • Grafana: http://localhost:3000 (admin/admin)"
  echo "  • Prometheus: http://localhost:9090"
  echo "  • Dynamic vLLM API: http://localhost:8000"  # ИСПРАВЛЕНО
  echo
  
  echo "📁 Директории:"
  echo "  • Входные PDF: ./input_pdf/"
  echo "  • Результаты (RU): ./output_md_ru/"
  echo "  • Результаты (EN): ./output_md_en/"
  echo "  • Результаты (ZH): ./output_md_zh/"
  echo "  • Логи: ./logs/"
  echo
  
  echo "🔧 Управление:"
  echo "  • Статус: docker compose ps"
  echo "  • Логи: docker compose logs -f [service]"
  echo "  • Остановка: docker compose down"
  echo "  • Перезапуск: ./start-system-fixed.sh"
  echo
  
  if [ -f "$PROJECT_DIR/convert_md.sh" ]; then
    echo "📝 Скрипты для работы:"
    [ -f "convert_md.sh" ] && echo "  • ./convert_md.sh - Конвертация PDF"
    [ -f "translate_ru.sh" ] && echo "  • ./translate_ru-fixed.sh - Перевод на русский"
    [ -f "translate_en.sh" ] && echo "  • ./translate_en-fixed.sh - Перевод на английский"
    [ -f "quality_check.sh" ] && echo "  • ./quality_check.sh - Проверка качества"
    echo
  fi
  
  log_success "Система готова к работе! 🚀"
}

# Обработка аргументов
case "${1:-start}" in
  "start")
    check_system_ready
    start_services
    if health_check; then
      show_info
    else
      echo
      log_warning "Система запущена, но некоторые сервисы могут требовать времени"
      log_info "Проверьте статус через 2-3 минуты: docker compose ps"
    fi
    ;;
  "stop")
    log_info "Остановка всех сервисов..."
    cd "$PROJECT_DIR"
    docker compose down
    log_success "Все сервисы остановлены"
    ;;
  "restart")
    log_info "Перезапуск системы..."
    cd "$PROJECT_DIR"
    docker compose down
    sleep 5
    $0 start
    ;;
  "status")
    log_info "Статус системы:"
    cd "$PROJECT_DIR"
    docker compose ps
    ;;
  "logs")
    cd "$PROJECT_DIR"
    if [ -n "${2:-}" ]; then
      # ИСПРАВЛЕНО: правильное имя сервиса для логов
      if [ "$2" == "vllm" ] || [ "$2" == "vllm-server" ]; then
        docker compose logs -f dynamic-vllm-server
      else
        docker compose logs -f "$2"
      fi
    else
      docker compose logs -f
    fi
    ;;
  "help"|"-h"|"--help")
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  start - Запуск системы (по умолчанию)"
    echo "  stop - Остановка системы"
    echo "  restart - Перезапуск системы"
    echo "  status - Показать статус контейнеров"
    echo "  logs - Показать логи (добавьте имя сервиса для конкретного)"
    echo "  help - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 # Запуск системы"
    echo "  $0 logs dynamic-vllm-server # Логи vLLM сервера"
    echo "  $0 status # Статус всех сервисов"
    ;;
  *)
    log_error "Неизвестная команда: $1"
    log_info "Используйте '$0 help' для справки"
    exit 1
    ;;
esac