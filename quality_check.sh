#!/usr/bin/env bash

# quality_check.sh - Проверка качества перевода v3.0

set -e

TRANSLATED_FILE="$1"

if [[ -z "$TRANSLATED_FILE" ]]; then
    echo "❌ Usage: ./quality_check.sh <translated_file.md>"
    exit 1
fi

if [[ ! -f "$TRANSLATED_FILE" ]]; then
    echo "❌ File not found: $TRANSLATED_FILE"
    exit 1
fi

echo "🔍 Quality check for: $TRANSLATED_FILE"
echo "======================================================"

# Проверяем остатки китайских символов
CHINESE_COUNT=$(grep -o '[\u4e00-\u9fff]' "$TRANSLATED_FILE" | wc -l)
echo "🇨🇳 Chinese characters remaining: $CHINESE_COUNT"

# Проверяем структуру таблиц
TABLE_LINES=$(grep -c '^|.*|$' "$TRANSLATED_FILE" || echo 0)
echo "📋 Table lines found: $TABLE_LINES"

# Проверяем защищенные команды
PROTECTED_COMMANDS=$(grep -c 'ipmitool\|chassis\|power\|mc\|sensor' "$TRANSLATED_FILE" || echo 0)
echo "⚡ Protected commands found: $PROTECTED_COMMANDS"

# Проверяем структуру файла
HEADERS=$(grep -c '^#' "$TRANSLATED_FILE" || echo 0)
echo "📋 Headers found: $HEADERS"

# Размер файла
FILE_SIZE=$(wc -c < "$TRANSLATED_FILE")
echo "📊 File size: $FILE_SIZE bytes"

# Общая оценка
if [[ $CHINESE_COUNT -eq 0 ]]; then
    echo "✅ Translation completeness: EXCELLENT"
elif [[ $CHINESE_COUNT -lt 10 ]]; then
    echo "🟡 Translation completeness: GOOD"
else
    echo "🔴 Translation completeness: NEEDS IMPROVEMENT"
fi

echo "🏆 Quality check completed!"