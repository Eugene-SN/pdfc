#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Docling Processor для PDF Converter Pipeline v4.0
Высокоуровневый layout parsing и извлечение структурированного контента из PDF
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path
import tempfile
import json
from dataclasses import dataclass
from datetime import datetime

# Docling импорты
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend

# Вспомогательные библиотеки
import structlog
import numpy as np
from PIL import Image
import pandas as pd

# Для работы с файловой системой
import shutil
from pathlib import Path

# Prometheus метрики
from prometheus_client import Counter, Histogram, Gauge

# =======================================================================================
# КОНФИГУРАЦИЯ И МЕТРИКИ
# =======================================================================================

logger = structlog.get_logger("docling_processor")

# Метрики Prometheus
docling_requests = Counter('docling_requests_total', 'Total Docling processing requests', ['status'])
docling_duration = Histogram('docling_processing_duration_seconds', 'Docling processing duration')
docling_pages = Histogram('docling_pages_processed', 'Pages processed by Docling')
docling_elements = Counter('docling_elements_extracted', 'Elements extracted by type', ['element_type'])

@dataclass
class DoclingConfig:
    """Конфигурация Docling процессора"""
    model_path: str = "/mnt/storage/models/shared/docling"
    use_gpu: bool = True
    max_workers: int = 4
    timeout: int = 300
    cache_dir: str = "/app/cache"
    temp_dir: str = "/app/temp"
    
    # Параметры обработки
    extract_images: bool = True
    extract_tables: bool = True
    extract_formulas: bool = True
    high_quality_ocr: bool = True
    
    # Специфические настройки для китайских документов
    chinese_language_support: bool = True
    preserve_chinese_layout: bool = True
    mixed_language_mode: bool = True

# =======================================================================================
# ОСНОВНЫЕ ТИПЫ ДАННЫХ
# =======================================================================================

@dataclass
class ProcessedElement:
    """Обработанный элемент документа"""
    element_type: str
    content: str
    bbox: Optional[Tuple[float, float, float, float]] = None
    page_number: int = 0
    confidence: float = 1.0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

@dataclass
class DocumentStructure:
    """Структура документа после обработки"""
    title: Optional[str] = None
    authors: List[str] = None
    sections: List[Dict[str, Any]] = None
    tables: List[Dict[str, Any]] = None
    images: List[Dict[str, Any]] = None
    formulas: List[Dict[str, Any]] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.authors is None:
            self.authors = []
        if self.sections is None:
            self.sections = []
        if self.tables is None:
            self.tables = []
        if self.images is None:
            self.images = []
        if self.formulas is None:
            self.formulas = []
        if self.metadata is None:
            self.metadata = {}

# =======================================================================================
# DOCLING PROCESSOR КЛАСС
# =======================================================================================

class DoclingProcessor:
    """Главный класс для обработки PDF через Docling"""
    
    def __init__(self, config: Optional[DoclingConfig] = None):
        self.config = config or DoclingConfig()
        self.converter: Optional[DocumentConverter] = None
        self.logger = structlog.get_logger("docling_processor")
        
        # Создаем необходимые директории
        Path(self.config.cache_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.temp_dir).mkdir(parents=True, exist_ok=True)
        
        self._initialize_converter()
    
    def _initialize_converter(self):
        """Инициализация Docling конвертера"""
        try:
            # Правильная инициализация через DocumentConverter без прямого создания backend
            from docling.datamodel.base_models import InputFormat
            from docling.document_converter import PdfFormatOption
        
            # Настройка опций для PDF
            pdf_format_options = PdfFormatOption(
                backend=PyPdfiumDocumentBackend,  # Передаем класс, а не экземпляр
                pipeline_options=PdfPipelineOptions(
                    images_scale=2.0,
                    generate_page_images=True,
                    generate_table_images=True,
                    generate_picture_images=True
                )
            )
        
            # Создание конвертера с правильной конфигурацией
            self.converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: pdf_format_options
                }
            )
            
            self.logger.info("Docling converter initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Docling converter: {e}")
            raise
    
    async def process_document(self, pdf_path: str, output_dir: str) -> DocumentStructure:
        """
        Основной метод обработки PDF документа
        
        Args:
            pdf_path: Путь к PDF файлу
            output_dir: Директория для сохранения результатов
            
        Returns:
            DocumentStructure: Структурированные данные документа
        """
        start_time = datetime.now()
        
        try:
            docling_requests.labels(status='started').inc()
            
            # Проверяем существование файла
            if not Path(pdf_path).exists():
                raise FileNotFoundError(f"PDF file not found: {pdf_path}")
            
            # Создаем временную директорию для обработки
            with tempfile.TemporaryDirectory(dir=self.config.temp_dir) as temp_dir:
                # Конвертируем документ
                self.logger.info(f"Starting Docling conversion for: {pdf_path}")
                
                conv_result = self.converter.convert(pdf_path)
                
                # Извлекаем данные из результата конверсии
                document_structure = await self._extract_document_structure(
                    conv_result, temp_dir, output_dir
                )
                
                # Обновляем метрики
                duration = (datetime.now() - start_time).total_seconds()
                docling_duration.observe(duration)
                docling_pages.observe(len(conv_result.document.pages))
                docling_requests.labels(status='completed').inc()
                
                self.logger.info(
                    f"Document processed successfully in {duration:.2f}s",
                    pages=len(conv_result.document.pages),
                    elements=len(document_structure.sections) + len(document_structure.tables)
                )
                
                return document_structure
                
        except Exception as e:
            docling_requests.labels(status='error').inc()
            self.logger.error(f"Error processing document {pdf_path}: {e}")
            raise
    
    async def _extract_document_structure(
        self, 
        conv_result, 
        temp_dir: str, 
        output_dir: str
    ) -> DocumentStructure:
        """Извлечение структурированных данных из результата Docling"""
        
        document = conv_result.document
        structure = DocumentStructure()
        
        # Извлечение метаданных
        structure.metadata = {
            "total_pages": len(document.pages),
            "processing_time": datetime.now().isoformat(),
            "docling_version": "1.5.0",
            "language_detected": "mixed",  # TODO: реальная детекция языка
        }
        
        # Извлечение заголовка из первых элементов
        structure.title = await self._extract_title(document)
        
        # Извлечение разделов и текстового контента
        structure.sections = await self._extract_sections(document)
        
        # Извлечение таблиц
        if self.config.extract_tables:
            structure.tables = await self._extract_tables(document, temp_dir, output_dir)
        
        # Извлечение изображений  
        if self.config.extract_images:
            structure.images = await self._extract_images(document, temp_dir, output_dir)
        
        # Извлечение формул
        if self.config.extract_formulas:
            structure.formulas = await self._extract_formulas(document)
        
        return structure
    
    async def _extract_title(self, document) -> Optional[str]:
        """Извлечение заголовка документа"""
        try:
            # Ищем заголовок в первых элементах первой страницы
            if document.pages and len(document.pages) > 0:
                first_page = document.pages[0]
                
                for element in first_page.elements[:5]:  # Проверяем первые 5 элементов
                    if hasattr(element, 'label') and element.label in ['title', 'heading', 'h1']:
                        if hasattr(element, 'text') and element.text:
                            return element.text.strip()
                            
                # Если явного заголовка нет, берем первый текстовый блок
                for element in first_page.elements:
                    if hasattr(element, 'text') and element.text:
                        text = element.text.strip()
                        if len(text) < 200:  # Предполагаем, что заголовок короткий
                            return text
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to extract title: {e}")
            return None
    
    async def _extract_sections(self, document) -> List[Dict[str, Any]]:
        """Извлечение разделов и текстового контента"""
        sections = []
        current_section = None
        
        try:
            for page_num, page in enumerate(document.pages, 1):
                for element in page.elements:
                    element_type = getattr(element, 'label', 'unknown')
                    
                    # Обновляем метрики
                    docling_elements.labels(element_type=element_type).inc()
                    
                    if element_type in ['heading', 'title', 'h1', 'h2', 'h3']:
                        # Новый раздел
                        if current_section:
                            sections.append(current_section)
                        
                        current_section = {
                            'title': getattr(element, 'text', '').strip(),
                            'level': self._get_heading_level(element_type),
                            'page': page_num,
                            'content': '',
                            'subsections': []
                        }
                    
                    elif element_type in ['text', 'paragraph', 'body']:
                        # Текстовый контент
                        text = getattr(element, 'text', '').strip()
                        if text and current_section:
                            current_section['content'] += text + '\n\n'
                        elif text and not current_section:
                            # Контент без заголовка - создаем безымянный раздел
                            current_section = {
                                'title': f'Section (Page {page_num})',
                                'level': 1,
                                'page': page_num,
                                'content': text + '\n\n',
                                'subsections': []
                            }
            
            # Добавляем последний раздел
            if current_section:
                sections.append(current_section)
            
            self.logger.info(f"Extracted {len(sections)} sections from document")
            return sections
            
        except Exception as e:
            self.logger.error(f"Error extracting sections: {e}")
            return []
    
    def _get_heading_level(self, element_type: str) -> int:
        """Определение уровня заголовка"""
        level_map = {
            'title': 1,
            'h1': 1, 
            'heading': 1,
            'h2': 2,
            'h3': 3,
        }
        return level_map.get(element_type, 1)
    
    async def _extract_tables(
        self, 
        document, 
        temp_dir: str, 
        output_dir: str
    ) -> List[Dict[str, Any]]:
        """Извлечение таблиц"""
        tables = []
        
        try:
            table_counter = 0
            
            for page_num, page in enumerate(document.pages, 1):
                for element in page.elements:
                    if getattr(element, 'label', '') == 'table':
                        table_counter += 1
                        
                        # Извлечение данных таблицы
                        table_data = await self._process_table_element(
                            element, table_counter, page_num, temp_dir, output_dir
                        )
                        
                        if table_data:
                            tables.append(table_data)
            
            self.logger.info(f"Extracted {len(tables)} tables from document")
            return tables
            
        except Exception as e:
            self.logger.error(f"Error extracting tables: {e}")
            return []
    
    async def _process_table_element(
        self, 
        element, 
        table_id: int, 
        page_num: int, 
        temp_dir: str, 
        output_dir: str
    ) -> Optional[Dict[str, Any]]:
        """Обработка отдельного элемента таблицы"""
        try:
            # Получение табличных данных
            if hasattr(element, 'data') and element.data:
                # Преобразуем в DataFrame для удобства
                df = pd.DataFrame(element.data)
                
                # Сохраняем таблицу в CSV
                table_filename = f"table_{table_id}_page_{page_num}.csv"
                table_path = Path(output_dir) / table_filename
                df.to_csv(table_path, index=False, encoding='utf-8')
                
                return {
                    'id': table_id,
                    'page': page_num,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'file_path': str(table_path),
                    'data': df.to_dict('records'),
                    'bbox': getattr(element, 'bbox', None),
                    'confidence': getattr(element, 'confidence', 1.0)
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to process table {table_id}: {e}")
            return None
    
    async def _extract_images(
        self, 
        document, 
        temp_dir: str, 
        output_dir: str
    ) -> List[Dict[str, Any]]:
        """Извлечение изображений"""
        images = []
        
        try:
            image_counter = 0
            
            for page_num, page in enumerate(document.pages, 1):
                for element in page.elements:
                    if getattr(element, 'label', '') in ['figure', 'image', 'picture']:
                        image_counter += 1
                        
                        # Обработка изображения
                        image_data = await self._process_image_element(
                            element, image_counter, page_num, temp_dir, output_dir
                        )
                        
                        if image_data:
                            images.append(image_data)
            
            self.logger.info(f"Extracted {len(images)} images from document")
            return images
            
        except Exception as e:
            self.logger.error(f"Error extracting images: {e}")
            return []
    
    async def _process_image_element(
        self, 
        element, 
        image_id: int, 
        page_num: int, 
        temp_dir: str, 
        output_dir: str
    ) -> Optional[Dict[str, Any]]:
        """Обработка отдельного элемента изображения"""
        try:
            if hasattr(element, 'image') and element.image:
                # Сохранение изображения
                image_filename = f"image_{image_id}_page_{page_num}.png"
                image_path = Path(output_dir) / image_filename
                
                # Конвертация и сохранение
                if hasattr(element.image, 'save'):
                    element.image.save(str(image_path))
                
                return {
                    'id': image_id,
                    'page': page_num,
                    'file_path': str(image_path),
                    'bbox': getattr(element, 'bbox', None),
                    'caption': getattr(element, 'caption', ''),
                    'width': getattr(element.image, 'width', 0),
                    'height': getattr(element.image, 'height', 0)
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to process image {image_id}: {e}")
            return None
    
    async def _extract_formulas(self, document) -> List[Dict[str, Any]]:
        """Извлечение математических формул"""
        formulas = []
        
        try:
            formula_counter = 0
            
            for page_num, page in enumerate(document.pages, 1):
                for element in page.elements:
                    if getattr(element, 'label', '') in ['formula', 'equation', 'math']:
                        formula_counter += 1
                        
                        formulas.append({
                            'id': formula_counter,
                            'page': page_num,
                            'latex': getattr(element, 'text', ''),
                            'bbox': getattr(element, 'bbox', None),
                            'confidence': getattr(element, 'confidence', 1.0)
                        })
            
            self.logger.info(f"Extracted {len(formulas)} formulas from document")
            return formulas
            
        except Exception as e:
            self.logger.error(f"Error extracting formulas: {e}")
            return []
    
    def export_to_markdown(self, structure: DocumentStructure, output_path: str) -> str:
        """Экспорт структуры документа в Markdown"""
        try:
            md_content = []
            
            # Заголовок документа
            if structure.title:
                md_content.append(f"# {structure.title}\n")
            
            # Разделы
            for section in structure.sections:
                # Заголовок раздела
                level_prefix = "#" * section['level']
                md_content.append(f"{level_prefix} {section['title']}\n")
                
                # Содержание раздела
                if section['content']:
                    md_content.append(f"{section['content']}\n")
            
            # Таблицы
            if structure.tables:
                md_content.append("## Tables\n")
                for table in structure.tables:
                    md_content.append(f"### Table {table['id']} (Page {table['page']})\n")
                    md_content.append(f"Rows: {table['rows']}, Columns: {table['columns']}\n")
                    md_content.append(f"File: `{table['file_path']}`\n\n")
            
            # Изображения
            if structure.images:
                md_content.append("## Images\n")
                for image in structure.images:
                    md_content.append(f"### Image {image['id']} (Page {image['page']})\n")
                    md_content.append(f"![Image {image['id']}]({image['file_path']})\n")
                    if image.get('caption'):
                        md_content.append(f"*{image['caption']}*\n\n")
                    else:
                        md_content.append("\n")
            
            # Формулы
            if structure.formulas:
                md_content.append("## Formulas\n")
                for formula in structure.formulas:
                    md_content.append(f"### Formula {formula['id']} (Page {formula['page']})\n")
                    md_content.append(f"```latex\n{formula['latex']}\n```\n\n")
            
            # Сохранение в файл
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(md_content))
            
            self.logger.info(f"Markdown exported to: {output_path}")
            return '\n'.join(md_content)
            
        except Exception as e:
            self.logger.error(f"Error exporting to markdown: {e}")
            raise

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def create_docling_processor(config: Optional[DoclingConfig] = None) -> DoclingProcessor:
    """Фабричная функция для создания Docling процессора"""
    return DoclingProcessor(config)

async def process_pdf_with_docling(
    pdf_path: str, 
    output_dir: str,
    config: Optional[DoclingConfig] = None
) -> DocumentStructure:
    """
    Высокоуровневая функция для обработки PDF
    
    Args:
        pdf_path: Путь к PDF файлу
        output_dir: Директория для выходных файлов
        config: Конфигурация процессора
        
    Returns:
        DocumentStructure: Обработанный документ
    """
    processor = create_docling_processor(config)
    return await processor.process_document(pdf_path, output_dir)

# =======================================================================================
# ОСНОВНОЙ БЛОК ДЛЯ ТЕСТИРОВАНИЯ
# =======================================================================================

if __name__ == "__main__":
    # Пример использования
    async def main():
        config = DoclingConfig()
        processor = DoclingProcessor(config)
        
        # Тестовый файл (замените на реальный путь)
        pdf_path = "/app/temp/test_document.pdf"
        output_dir = "/app/temp/output"
        
        if Path(pdf_path).exists():
            structure = await processor.process_document(pdf_path, output_dir)
            
            # Экспорт в Markdown
            md_path = Path(output_dir) / "document.md"
            processor.export_to_markdown(structure, str(md_path))
            
            print(f"Document processed successfully!")
            print(f"Sections: {len(structure.sections)}")
            print(f"Tables: {len(structure.tables)}")
            print(f"Images: {len(structure.images)}")
        else:
            print(f"Test file not found: {pdf_path}")
    
    asyncio.run(main())