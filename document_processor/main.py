#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI Main Server для Document Processor Service v4.0
Основной HTTP сервер для обработки PDF документов через Docling, OCR и извлечение таблиц
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import tempfile
import json
import time
from datetime import datetime
import traceback

# FastAPI импорты
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import uvicorn

# Pydantic модели
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# HTTP клиенты
import httpx
import aiofiles

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server, generate_latest, REGISTRY
from prometheus_client.exposition import CONTENT_TYPE_LATEST
import psutil

# Наши процессоры
from docling_processor import DoclingProcessor, DoclingConfig, DocumentStructure
from ocr_processor import OCRProcessor, OCRConfig
from table_extractor import TableExtractor, TableConfig
from structure_analyzer import StructureAnalyzer, AnalysisConfig

# =======================================================================================
# КОНФИГУРАЦИЯ И НАСТРОЙКИ
# =======================================================================================

class Settings(BaseSettings):
    """Настройки приложения"""
    # Основные настройки сервера
    host: str = "0.0.0.0"
    port: int = 8001
    debug: bool = False
    
    # Пути
    temp_dir: str = "/app/temp"
    cache_dir: str = "/app/cache"
    models_dir: str = "/mnt/storage/models"

    # Пути к моделям
    paddlex_home: str = "/mnt/storage/models/paddlex"
    docling_models_dir: str = "/mnt/storage/models/docling"
    hf_cache_dir: str = "/mnt/storage/models/docling/huggingface"

    # Ограничения
    max_file_size: int = 500 * 1024 * 1024  # 500MB
    max_pages: int = 1000
    timeout_seconds: int = 600
    
    # Docling настройки
    docling_model_path: str = "/mnt/storage/models/docling"
    docling_use_gpu: bool = True
    docling_max_workers: int = 4
    
    # OCR настройки
    paddleocr_use_gpu: bool = True
    paddleocr_langs: List[str] = ["ch", "en", "ru"]  # Список языков
    ocr_confidence_threshold: float = 0.8
    
    # Таблицы настройки
    tabula_java_options: str = "-Xmx2048m"
    table_detection_threshold: float = 0.7
    
    class Config:
        env_file = ".env"

settings = Settings()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = structlog.get_logger("document_processor_api")

# =======================================================================================
# PROMETHEUS МЕТРИКИ
# =======================================================================================

def create_metric_safe(type_cls, name, description, labels=None):
    """Безопасное создание метрики - возвращает существующую или создает новую"""
    if name in REGISTRY._names_to_collectors:
        return REGISTRY._names_to_collectors[name]
    else:
        if labels:
            return type_cls(name, description, labels)
        else:
            return type_cls(name, description)

# HTTP метрики
http_requests = create_metric_safe(Counter, 'doc_processor_http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
http_duration = create_metric_safe(Histogram, 'doc_processor_http_duration_seconds', 'HTTP request duration', ['method', 'endpoint'])
active_requests = create_metric_safe(Gauge, 'doc_processor_active_requests', 'Active HTTP requests')

# Обработка файлов
files_processed = create_metric_safe(Counter, 'doc_processor_files_total', 'Total files processed', ['status', 'type'])
processing_duration = create_metric_safe(Histogram, 'doc_processor_processing_duration_seconds', 'File processing duration', ['type'])
pages_processed = create_metric_safe(Counter, 'doc_processor_pages_total', 'Total pages processed')

# Системные метрики
memory_usage = create_metric_safe(Gauge, 'doc_processor_memory_usage_bytes', 'Memory usage')
disk_usage = create_metric_safe(Gauge, 'doc_processor_disk_usage_percent', 'Disk usage percentage')

# =======================================================================================
# PYDANTIC МОДЕЛИ
# =======================================================================================

class ProcessingOptions(BaseModel):
    """Опции для обработки документа"""
    extract_tables: bool = True
    extract_images: bool = True
    extract_formulas: bool = True
    use_ocr: bool = True
    high_quality_ocr: bool = True
    output_format: str = Field(default="json", pattern="^(json|markdown)$")
    language: str = "zh-CN"

class ProcessingResponse(BaseModel):
    """Ответ на запрос обработки"""
    success: bool
    message: str
    processing_time: float
    document_id: str
    pages_count: int
    sections_count: int
    tables_count: int
    images_count: int
    formulas_count: int
    output_files: List[str]
    metadata: Dict[str, Any]

class HealthResponse(BaseModel):
    """Ответ health check"""
    status: str
    timestamp: str
    version: str = "4.0.0"
    services: Dict[str, str]
    system_info: Dict[str, Any]

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def update_system_metrics():
    """Обновление системных метрик"""
    try:
        # Память
        memory = psutil.virtual_memory()
        memory_usage.set(memory.used)
        
        # Диск
        disk = psutil.disk_usage('/app/temp')
        disk_usage.set(disk.percent)
        
    except Exception as e:
        logger.warning(f"Failed to update system metrics: {e}")

def validate_pdf_file(file_content: bytes) -> bool:
    """Валидация PDF файла"""
    # Проверяем размер
    if len(file_content) > settings.max_file_size:
        raise HTTPException(
            status_code=413, 
            detail=f"File too large. Max size: {settings.max_file_size} bytes"
        )
    
    # Проверяем PDF signature
    if not file_content.startswith(b'%PDF'):
        raise HTTPException(
            status_code=400,
            detail="Invalid PDF file format"
        )
    
    return True

async def save_uploaded_file(upload_file: UploadFile, temp_dir: str) -> str:
    """Сохранение загруженного файла"""
    # Создаем временный файл
    file_path = Path(temp_dir) / f"upload_{int(time.time())}_{upload_file.filename}"
    
    try:
        async with aiofiles.open(file_path, 'wb') as f:
            content = await upload_file.read()
            validate_pdf_file(content)
            await f.write(content)
        
        return str(file_path)
        
    except Exception as e:
        if file_path.exists():
            file_path.unlink()
        raise

# =======================================================================================
# ИНИЦИАЛИЗАЦИЯ ПРОЦЕССОРОВ
# =======================================================================================

# Глобальные процессоры (инициализируются при запуске)
docling_processor: Optional[DoclingProcessor] = None
ocr_processor: Optional[OCRProcessor] = None
table_extractor: Optional[TableExtractor] = None
structure_analyzer: Optional[StructureAnalyzer] = None

async def initialize_processors():
    """Инициализация всех процессоров"""
    global docling_processor, ocr_processor, table_extractor, structure_analyzer
    
    logger.info("Initializing document processors...")
    
    try:
        # Docling
        docling_config = DoclingConfig(
            model_path=settings.docling_model_path,
            use_gpu=settings.docling_use_gpu,
            max_workers=settings.docling_max_workers,
            cache_dir=settings.cache_dir,
            temp_dir=settings.temp_dir
        )
        docling_processor = DoclingProcessor(docling_config)
        
        # OCR
        ocr_config = OCRConfig(
            use_gpu=settings.paddleocr_use_gpu,
            lang=settings.paddleocr_langs,
            confidence_threshold=settings.ocr_confidence_threshold,
        )
        ocr_processor = OCRProcessor(ocr_config)
        
        # Tables
        table_config = TableConfig(
            java_options=settings.tabula_java_options,
            detection_threshold=settings.table_detection_threshold,
            temp_dir=settings.temp_dir
        )
        table_extractor = TableExtractor(table_config)
        
        # Structure Analysis
        analysis_config = AnalysisConfig(
            min_heading_length=5,
            max_heading_length=200,
            check_cross_references=True
        )
        structure_analyzer = StructureAnalyzer(analysis_config)
        
        logger.info("All processors initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize processors: {e}")
        raise

# =======================================================================================
# FASTAPI APPLICATION
# =======================================================================================

app = FastAPI(
    title="Document Processor API",
    description="PDF document processing with Docling, OCR, and table extraction",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# =======================================================================================
# API ENDPOINTS
# =======================================================================================

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    logger.info("Starting Document Processor API v4.0")
    
    # Создаем директории
    Path(settings.temp_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.cache_dir).mkdir(parents=True, exist_ok=True)
    
    # Инициализируем процессоры
    await initialize_processors()
    
    # Запускаем Prometheus метрики
    start_http_server(8002)
    logger.info("Prometheus metrics server started on port 8002")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    update_system_metrics()
    
    # Проверяем статус сервисов
    services_status = {
        "docling": "healthy" if docling_processor else "unavailable",
        "ocr": "healthy" if ocr_processor else "unavailable", 
        "table_extractor": "healthy" if table_extractor else "unavailable",
        "structure_analyzer": "healthy" if structure_analyzer else "unavailable"
    }
    
    # Системная информация
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/app/temp')
    
    system_info = {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": memory.percent,
        "memory_available_gb": round(memory.available / 1024**3, 2),
        "disk_free_gb": round(disk.free / 1024**3, 2),
        "temp_files_count": len(list(Path(settings.temp_dir).glob("*"))),
        "uptime_seconds": int(time.time() - startup_time)
    }
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        services=services_status,
        system_info=system_info
    )

@app.post("/convert", response_model=ProcessingResponse)
async def convert_document(
    file: UploadFile = File(...),
    options: str = Form(default='{"extract_tables": true, "extract_images": true, "use_ocr": true}')
):
    """
    Конвертация PDF документа в структурированный JSON
    """
    start_time = time.time()
    active_requests.inc()
    document_id = f"doc_{int(start_time)}"
    
    try:
        http_requests.labels(method="POST", endpoint="/convert", status="started").inc()
        
        # Парсим опции
        try:
            processing_options = ProcessingOptions.parse_raw(options)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid options format: {e}")
        
        # Создаем рабочую директорию
        work_dir = Path(settings.temp_dir) / document_id
        work_dir.mkdir(parents=True, exist_ok=True)
        
        # Сохраняем файл
        pdf_path = await save_uploaded_file(file, str(work_dir))
        
        logger.info(f"Starting document conversion: {document_id}")
        
        # Основная обработка через Docling
        document_structure = await docling_processor.process_document(
            pdf_path, str(work_dir)
        )
        
        # Дополнительная обработка OCR если нужно
        if processing_options.use_ocr and ocr_processor:
            ocr_results = await ocr_processor.process_document_pages(
                pdf_path, str(work_dir)
            )
            document_structure.metadata["ocr_results"] = ocr_results
        
        # Улучшенное извлечение таблиц
        if processing_options.extract_tables and table_extractor:
            enhanced_tables = await table_extractor.extract_tables_from_pdf(
                pdf_path, str(work_dir)
            )
            # Объединяем с результатами Docling
            document_structure.tables.extend(enhanced_tables)
        
        # Структурный анализ
        if structure_analyzer:
            analysis_result = await structure_analyzer.analyze_document_structure(
                document_structure
            )
            document_structure.metadata["structure_analysis"] = analysis_result
        
        # Подготовка ответа
        processing_time = time.time() - start_time
        
        # Сохраняем результат в JSON
        result_file = work_dir / "document_structure.json"
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump({
                "title": document_structure.title,
                "authors": document_structure.authors,
                "sections": document_structure.sections,
                "tables": [{"id": t["id"], "page": t["page"], "file_path": t["file_path"]} for t in document_structure.tables],
                "images": [{"id": i["id"], "page": i["page"], "file_path": i["file_path"]} for i in document_structure.images],
                "formulas": document_structure.formulas,
                "metadata": document_structure.metadata
            }, f, ensure_ascii=False, indent=2)
        
        # Метрики
        processing_duration.labels(type="convert").observe(processing_time)
        pages_processed.inc(document_structure.metadata.get("total_pages", 0))
        files_processed.labels(status="success", type="pdf").inc()
        http_requests.labels(method="POST", endpoint="/convert", status="success").inc()
        
        response = ProcessingResponse(
            success=True,
            message="Document converted successfully",
            processing_time=processing_time,
            document_id=document_id,
            pages_count=document_structure.metadata.get("total_pages", 0),
            sections_count=len(document_structure.sections),
            tables_count=len(document_structure.tables),
            images_count=len(document_structure.images),
            formulas_count=len(document_structure.formulas),
            output_files=[str(result_file)],
            metadata=document_structure.metadata
        )
        
        logger.info(f"Document conversion completed: {document_id} in {processing_time:.2f}s")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        files_processed.labels(status="error", type="pdf").inc()
        http_requests.labels(method="POST", endpoint="/convert", status="error").inc()
        logger.error(f"Error converting document {document_id}: {e}\n{traceback.format_exc()}")
        
        raise HTTPException(
            status_code=500,
            detail=f"Document conversion failed: {str(e)}"
        )
        
    finally:
        active_requests.dec()

@app.post("/markdown")
async def convert_to_markdown(
    file: UploadFile = File(...),
    options: str = Form(default='{"extract_tables": true, "extract_images": true}')
):
    """
    Конвертация PDF в Markdown формат
    """
    start_time = time.time()
    active_requests.inc()
    document_id = f"md_{int(start_time)}"
    
    try:
        # Сначала конвертируем в JSON
        json_result = await convert_document(file, options)
        
        # Загружаем результат для конвертации в MD
        work_dir = Path(settings.temp_dir) / json_result.document_id
        json_file = work_dir / "document_structure.json"
        
        with open(json_file, 'r', encoding='utf-8') as f:
            document_data = json.load(f)
        
        # Создаем DocumentStructure из JSON
        document_structure = DocumentStructure(
            title=document_data["title"],
            authors=document_data["authors"],
            sections=document_data["sections"],
            tables=document_data["tables"],
            images=document_data["images"], 
            formulas=document_data["formulas"],
            metadata=document_data["metadata"]
        )
        
        # Конвертируем в Markdown
        md_file = work_dir / "document.md"
        markdown_content = docling_processor.export_to_markdown(document_structure, str(md_file))
        
        processing_duration.labels(type="markdown").observe(time.time() - start_time)
        
        return FileResponse(
            path=str(md_file),
            filename=f"{document_id}.md",
            media_type="text/markdown"
        )
        
    except Exception as e:
        logger.error(f"Error converting to markdown: {e}")
        raise HTTPException(status_code=500, detail=f"Markdown conversion failed: {str(e)}")
    finally:
        active_requests.dec()

@app.get("/metrics")
async def get_metrics():
    """Prometheus метрики endpoint"""
    from fastapi import Response
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/status")
async def get_status():
    """Подробный статус всех компонентов"""
    return {
        "service": "document-processor",
        "version": "4.0.0", 
        "timestamp": datetime.now().isoformat(),
        "processors": {
            "docling": bool(docling_processor),
            "ocr": bool(ocr_processor),
            "table_extractor": bool(table_extractor),
            "structure_analyzer": bool(structure_analyzer)
        },
        "settings": {
            "max_file_size_mb": settings.max_file_size / 1024 / 1024,
            "timeout_seconds": settings.timeout_seconds,
            "temp_dir": settings.temp_dir
        }
    }

# =======================================================================================
# MAIN
# =======================================================================================

startup_time = time.time()

if __name__ == "__main__":
    logger.info(f"Starting Document Processor API on {settings.host}:{settings.port}")
    
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        log_level="info" if not settings.debug else "debug",
        access_log=True,
        reload=settings.debug
    )