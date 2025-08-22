#!/bin/bash
# PDF to Markdown Converter with Progress Bar
# Converts PDF files to Markdown format without translation
# Usage: ./convert_md.sh [pdf_file] or ./convert_md.sh (for batch processing)

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/input_pdf"
OUTPUT_DIR="${SCRIPT_DIR}/output_md_zh"
LOGS_DIR="${SCRIPT_DIR}/logs"
CONFIG_FILE="${SCRIPT_DIR}/.env"

# Colors for output (no emojis as requested)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load configuration
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
fi

# Default service URLs
DOCUMENT_PROCESSOR_URL=${DOCUMENT_PROCESSOR_URL:-"http://localhost:8001"}
MAX_RETRIES=${MAX_RETRIES:-3}
RETRY_DELAY=${RETRY_DELAY:-5}

# Logging setup
LOG_FILE="${LOGS_DIR}/convert_md_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$LOGS_DIR"

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Progress bar function
show_progress() {
    local current=$1
    local total=$2
    local width=50
    local percentage=$((current * 100 / total))
    local completed=$((current * width / total))
    local remaining=$((width - completed))
    
    printf "\rProgress: ["
    printf "%*s" "$completed" '' | tr ' ' '='
    printf "%*s" "$remaining" '' | tr ' ' '-'
    printf "] %d%% (%d/%d)" "$percentage" "$current" "$total"
}

# Check service health
check_service() {
    local service_url="$1"
    local service_name="$2"
    local max_attempts=30
    local attempt=1
    
    log "INFO" "Checking $service_name health at $service_url"
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$service_url/health" > /dev/null 2>&1; then
            log "INFO" "$service_name is healthy"
            return 0
        fi
        
        printf "\rWaiting for %s... [%d/%d]" "$service_name" "$attempt" "$max_attempts"
        sleep 2
        ((attempt++))
    done
    
    log "ERROR" "$service_name is not available after $max_attempts attempts"
    return 1
}

# Convert single PDF file
convert_pdf() {
    local pdf_file="$1"
    local output_file="$2"
    local attempt=1
    
    log "INFO" "Starting conversion: $pdf_file -> $output_file"
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log "INFO" "Conversion attempt $attempt/$MAX_RETRIES"
        
        # Prepare request payload
        local payload=$(cat <<EOF
{
    "input_file_path": "$pdf_file",
    "output_dir": "$OUTPUT_DIR",
    "conversion_format": "markdown",
    "options": {
        "extract_tables": true,
        "extract_images": true,
        "ocr_enabled": true,
        "preserve_layout": true,
        "extract_metadata": true
    },
    "processing_context": {
        "script": "convert_md.sh",
        "timestamp": "$(date -Iseconds)"
    }
}
EOF
)
        
        # Make conversion request
        local start_time=$(date +%s)
        local response=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "X-Request-ID: convert_$(basename "$pdf_file" .pdf)_$(date +%s)" \
            -d "$payload" \
            "$DOCUMENT_PROCESSOR_URL/convert" 2>/dev/null)
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        if [ $? -eq 0 ] && echo "$response" | grep -q '"success": *true'; then
            log "INFO" "Conversion successful in ${duration}s"
            
            # Extract markdown content
            local markdown_content=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('markdown_content', ''))
except:
    pass
")
            
            if [ -n "$markdown_content" ]; then
                echo "$markdown_content" > "$output_file"
                
                # Log conversion statistics
                local stats=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    stats = data.get('stats', {})
    print(f\"Pages: {stats.get('pages_processed', 'N/A')}\")
    print(f\"Tables: {len(data.get('tables', []))}\")
    print(f\"Images: {len(data.get('images', []))}\")
    print(f\"Words: {stats.get('words_count', 'N/A')}\")
    print(f\"Processing time: {stats.get('processing_time', 'N/A')}s\")
except:
    print('Stats unavailable')
" | tr '\n' ', ')
                
                log "INFO" "Conversion statistics: $stats"
                return 0
            else
                log "WARN" "No markdown content in response"
            fi
        else
            local error_msg=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('message', 'Unknown error'))
except:
    print('Invalid response format')
" 2>/dev/null || echo "Request failed")
            
            log "WARN" "Conversion attempt $attempt failed: $error_msg"
        fi
        
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "INFO" "Retrying in ${RETRY_DELAY}s..."
            sleep $RETRY_DELAY
        fi
        
        ((attempt++))
    done
    
    log "ERROR" "All conversion attempts failed for $pdf_file"
    return 1
}

# Main conversion function
main() {
    local start_time=$(date +%s)
    
    echo -e "${BLUE}PDF to Markdown Converter v4.1.0${NC}"
    echo "======================================"
    echo
    
    # Check if directories exist
    if [ ! -d "$INPUT_DIR" ]; then
        log "ERROR" "Input directory not found: $INPUT_DIR"
        exit 1
    fi
    
    mkdir -p "$OUTPUT_DIR"
    
    # Check service health
    if ! check_service "$DOCUMENT_PROCESSOR_URL" "Document Processor"; then
        log "ERROR" "Document Processor service is not available"
        exit 1
    fi
    echo
    
    # Process files
    if [ $# -eq 1 ] && [ -f "$1" ]; then
        # Single file mode
        local pdf_file="$1"
        local output_file="$OUTPUT_DIR/$(basename "$pdf_file" .pdf).md"
        
        log "INFO" "Single file mode: $pdf_file"
        echo "Processing: $(basename "$pdf_file")"
        
        if convert_pdf "$pdf_file" "$output_file"; then
            echo -e "\n${GREEN}SUCCESS: Converted successfully${NC}"
            echo "Output: $output_file"
        else
            echo -e "\n${RED}ERROR: Conversion failed${NC}"
            exit 1
        fi
    else
        # Batch mode
        local pdf_files=()
        while IFS= read -r -d '' file; do
            pdf_files+=("$file")
        done < <(find "$INPUT_DIR" -name "*.pdf" -type f -print0)
        
        local total_files=${#pdf_files[@]}
        
        if [ $total_files -eq 0 ]; then
            log "WARN" "No PDF files found in $INPUT_DIR"
            echo "No PDF files found for processing"
            exit 0
        fi
        
        log "INFO" "Batch mode: processing $total_files files"
        echo "Found $total_files PDF files for processing"
        echo
        
        local processed=0
        local failed=0
        
        for pdf_file in "${pdf_files[@]}"; do
            local filename=$(basename "$pdf_file")
            local output_file="$OUTPUT_DIR/${filename%.pdf}.md"
            
            echo "Processing: $filename"
            
            if convert_pdf "$pdf_file" "$output_file"; then
                ((processed++))
                echo -e "Status: ${GREEN}SUCCESS${NC}"
            else
                ((failed++))
                echo -e "Status: ${RED}FAILED${NC}"
            fi
            
            show_progress $((processed + failed)) $total_files
            echo
        done
        
        echo
        echo "======================================"
        echo -e "Batch processing completed"
        echo -e "Processed: ${GREEN}$processed${NC} files"
        echo -e "Failed: ${RED}$failed${NC} files"
    fi
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    log "INFO" "Conversion completed in ${total_duration}s"
    echo "Total time: ${total_duration}s"
    echo "Log file: $LOG_FILE"
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi