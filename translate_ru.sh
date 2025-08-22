#!/bin/bash
# PDF to Russian Translation Pipeline with Progress Bar
# Converts PDF to Markdown and translates to Russian using Qwen3-30B-A3B
# Usage: ./translate_ru.sh [pdf_file] or ./translate_ru.sh (for batch processing)

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/input_pdf"
OUTPUT_DIR="${SCRIPT_DIR}/output_md_ru"
LOGS_DIR="${SCRIPT_DIR}/logs"
CONFIG_FILE="${SCRIPT_DIR}/.env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Load configuration
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
fi

# Service URLs
DOCUMENT_PROCESSOR_URL=${DOCUMENT_PROCESSOR_URL:-"http://localhost:8001"}
VLLM_SERVER_URL=${VLLM_SERVER_URL:-"http://localhost:8000"}
QUALITY_ASSURANCE_URL=${QUALITY_ASSURANCE_URL:-"http://localhost:8002"}

# A3B Model Configuration
VLLM_MODEL_ALIAS=${VLLM_MODEL_ALIAS:-"Qwen3-30B-A3B-Instruct"}
A3B_TEMPERATURE=${A3B_TEMPERATURE:-"0.1"}
A3B_MAX_TOKENS=${A3B_MAX_TOKENS:-"6144"}
A3B_TOP_P=${A3B_TOP_P:-"0.8"}

# Translation parameters
SOURCE_LANGUAGE="english"
TARGET_LANGUAGE="russian"
MAX_RETRIES=${MAX_RETRIES:-3}
RETRY_DELAY=${RETRY_DELAY:-5}

# Logging setup
LOG_FILE="${LOGS_DIR}/translate_ru_$(date +%Y%m%d_%H%M%S).log"
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

# Check services health
check_services() {
    local services=(
        "$DOCUMENT_PROCESSOR_URL:Document Processor"
        "$VLLM_SERVER_URL:vLLM A3B Server"
        "$QUALITY_ASSURANCE_URL:Quality Assurance"
    )
    
    for service_info in "${services[@]}"; do
        local url="${service_info%:*}"
        local name="${service_info#*:}"
        local max_attempts=30
        local attempt=1
        
        log "INFO" "Checking $name health at $url"
        
        while [ $attempt -le $max_attempts ]; do
            if curl -s "$url/health" > /dev/null 2>&1; then
                log "INFO" "$name is healthy"
                break
            fi
            
            if [ $attempt -eq $max_attempts ]; then
                log "ERROR" "$name is not available after $max_attempts attempts"
                return 1
            fi
            
            printf "\rWaiting for %s... [%d/%d]" "$name" "$attempt" "$max_attempts"
            sleep 2
            ((attempt++))
        done
    done
    
    echo
    return 0
}

# Convert PDF to Markdown
convert_to_markdown() {
    local pdf_file="$1"
    local attempt=1
    
    log "INFO" "Converting PDF to Markdown: $pdf_file"
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log "INFO" "Conversion attempt $attempt/$MAX_RETRIES"
        
        local payload=$(cat <<EOF
{
    "input_file_path": "$pdf_file",
    "conversion_format": "markdown",
    "options": {
        "extract_tables": true,
        "extract_images": true,
        "ocr_enabled": true,
        "preserve_layout": true
    },
    "processing_context": {
        "script": "translate_ru.sh",
        "target_language": "$TARGET_LANGUAGE",
        "timestamp": "$(date -Iseconds)"
    }
}
EOF
)
        
        local start_time=$(date +%s)
        local response=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "X-Request-ID: convert_ru_$(basename "$pdf_file" .pdf)_$(date +%s)" \
            -d "$payload" \
            "$DOCUMENT_PROCESSOR_URL/convert" 2>/dev/null)
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        if [ $? -eq 0 ] && echo "$response" | grep -q '"success": *true'; then
            log "INFO" "PDF conversion successful in ${duration}s"
            
            local markdown_content=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('markdown_content', ''))
except:
    pass
")
            
            if [ -n "$markdown_content" ]; then
                echo "$markdown_content"
                return 0
            fi
        fi
        
        if [ $attempt -lt $MAX_RETRIES ]; then
            sleep $RETRY_DELAY
        fi
        ((attempt++))
    done
    
    log "ERROR" "PDF conversion failed after all attempts"
    return 1
}

# Translate markdown using A3B model
translate_markdown() {
    local markdown_content="$1"
    local attempt=1
    
    log "INFO" "Translating markdown content ($SOURCE_LANGUAGE -> $TARGET_LANGUAGE)"
    
    # A3B optimized system prompt for Russian translation
    local system_prompt="You are an expert technical translator using advanced A3B architecture for precise document translation.

TRANSLATION TASK: English → Russian

CORE PRINCIPLES:
• Preserve ALL technical terms, commands, and code blocks exactly as written
• Maintain perfect markdown formatting structure
• Translate descriptive text to natural, fluent Russian
• Keep technical terminology in original language when appropriate

CRITICAL PRESERVATION:
• Technical terms: IPMI, BMC, API, REST, JSON, XML, HTTP, CLI, SSH, URL
• Command syntax: ipmitool, curl, file paths, parameters
• Code blocks: \`\`\`bash, \`\`\`json, \`\`\`python etc.
• Formatting: tables, lists, headers, links

OUTPUT REQUIREMENT: Provide ONLY the translated document with no additional commentary."

    local user_prompt="Translate this technical document:

$markdown_content"
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log "INFO" "Translation attempt $attempt/$MAX_RETRIES using A3B model"
        
        local payload=$(cat <<EOF
{
    "model": "$VLLM_MODEL_ALIAS",
    "messages": [
        {
            "role": "system",
            "content": "$system_prompt"
        },
        {
            "role": "user", 
            "content": "$user_prompt"
        }
    ],
    "temperature": $A3B_TEMPERATURE,
    "max_tokens": $A3B_MAX_TOKENS,
    "top_p": $A3B_TOP_P,
    "stop": ["<|endoftext|>", "<|im_end|>"]
}
EOF
)
        
        local start_time=$(date +%s)
        local response=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "X-Request-ID: translate_ru_$(date +%s)" \
            -d "$payload" \
            "$VLLM_SERVER_URL/v1/chat/completions" 2>/dev/null)
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        if [ $? -eq 0 ]; then
            local translated_content=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    choices = data.get('choices', [])
    if choices:
        content = choices[0]['message']['content']
        print(content)
except:
    pass
" 2>/dev/null)
            
            if [ -n "$translated_content" ]; then
                log "INFO" "Translation successful in ${duration}s using A3B model"
                
                # Log usage statistics
                local usage_info=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    usage = data.get('usage', {})
    print(f\"Total tokens: {usage.get('total_tokens', 'N/A')}\")
    print(f\"Prompt tokens: {usage.get('prompt_tokens', 'N/A')}\")
    print(f\"Completion tokens: {usage.get('completion_tokens', 'N/A')}\")
except:
    print('Usage stats unavailable')
" 2>/dev/null | tr '\n' ', ')
                
                log "INFO" "A3B translation statistics: $usage_info"
                echo "$translated_content"
                return 0
            fi
        fi
        
        log "WARN" "Translation attempt $attempt failed"
        if [ $attempt -lt $MAX_RETRIES ]; then
            sleep $RETRY_DELAY
        fi
        ((attempt++))
    done
    
    log "ERROR" "Translation failed after all attempts"
    return 1
}

# Quality assurance validation
validate_translation() {
    local original_content="$1"
    local translated_content="$2"
    local pdf_file="$3"
    
    log "INFO" "Running quality assurance validation"
    
    local payload=$(cat <<EOF
{
    "document_id": "translate_ru_$(basename "$pdf_file" .pdf)",
    "original_pdf_path": "$pdf_file",
    "original_markdown": "$original_content",
    "translated_markdown": "$translated_content",
    "source_language": "$SOURCE_LANGUAGE",
    "target_language": "$TARGET_LANGUAGE",
    "validation_options": {
        "enable_auto_correction": true,
        "qa_threshold": 0.85,
        "check_formatting": true,
        "check_technical_terms": true,
        "check_completeness": true
    }
}
EOF
)
    
    local start_time=$(date +%s)
    local response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "X-Request-ID: qa_ru_$(date +%s)" \
        -d "$payload" \
        "$QUALITY_ASSURANCE_URL/validate" 2>/dev/null)
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $? -eq 0 ] && echo "$response" | grep -q '"success": *true'; then
        log "INFO" "Quality assurance completed in ${duration}s"
        
        # Extract QA results
        local qa_results=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    qa_score = data.get('overall_score', 0)
    passed = data.get('passed', False)
    corrected = data.get('corrected_document')
    
    print(f\"QA Score: {qa_score:.3f}\")
    print(f\"Passed: {passed}\")
    print(f\"Auto-corrected: {'Yes' if corrected else 'No'}\")
    
    # Return corrected version if available, otherwise original translation
    if corrected:
        print('CORRECTED_CONTENT:')
        print(corrected)
    else:
        print('ORIGINAL_TRANSLATION:')
        print('$translated_content')
except Exception as e:
    print('QA validation failed')
    print('ORIGINAL_TRANSLATION:')
    print('$translated_content')
" 2>/dev/null)
        
        log "INFO" "QA validation results: $(echo "$qa_results" | head -3 | tr '\n' ', ')"
        
        # Extract final content (after CORRECTED_CONTENT: or ORIGINAL_TRANSLATION:)
        local final_content=$(echo "$qa_results" | sed -n '/^CORRECTED_CONTENT:/,${/^CORRECTED_CONTENT:/d; p;}' | sed -n '/^ORIGINAL_TRANSLATION:/,${/^ORIGINAL_TRANSLATION:/d; p;}')
        
        echo "$final_content"
        return 0
    else
        log "WARN" "QA validation failed, using original translation"
        echo "$translated_content"
        return 0
    fi
}

# Process single PDF file
process_pdf() {
    local pdf_file="$1"
    local output_file="$2"
    
    log "INFO" "Starting Russian translation pipeline: $pdf_file"
    
    # Step 1: Convert PDF to Markdown
    echo "Step 1/3: Converting PDF to Markdown..."
    local markdown_content
    if ! markdown_content=$(convert_to_markdown "$pdf_file"); then
        log "ERROR" "PDF conversion failed"
        return 1
    fi
    
    # Step 2: Translate using A3B model
    echo "Step 2/3: Translating to Russian using A3B model..."
    local translated_content
    if ! translated_content=$(translate_markdown "$markdown_content"); then
        log "ERROR" "Translation failed"
        return 1
    fi
    
    # Step 3: Quality assurance
    echo "Step 3/3: Quality assurance validation..."
    local final_content
    if ! final_content=$(validate_translation "$markdown_content" "$translated_content" "$pdf_file"); then
        log "ERROR" "QA validation failed"
        return 1
    fi
    
    # Save final result
    echo "$final_content" > "$output_file"
    log "INFO" "Russian translation completed successfully: $output_file"
    
    return 0
}

# Main function
main() {
    local start_time=$(date +%s)
    
    echo -e "${BLUE}PDF to Russian Translation Pipeline v4.1.0 (A3B)${NC}"
    echo "=================================================="
    echo
    
    # Check directories
    if [ ! -d "$INPUT_DIR" ]; then
        log "ERROR" "Input directory not found: $INPUT_DIR"
        exit 1
    fi
    
    mkdir -p "$OUTPUT_DIR"
    
    # Check services
    if ! check_services; then
        log "ERROR" "One or more services are not available"
        exit 1
    fi
    
    # Process files
    if [ $# -eq 1 ] && [ -f "$1" ]; then
        # Single file mode
        local pdf_file="$1"
        local output_file="$OUTPUT_DIR/$(basename "$pdf_file" .pdf)_ru.md"
        
        log "INFO" "Single file mode: $pdf_file"
        echo "Processing: $(basename "$pdf_file") -> Russian"
        
        if process_pdf "$pdf_file" "$output_file"; then
            echo -e "\n${GREEN}SUCCESS: Russian translation completed${NC}"
            echo "Output: $output_file"
        else
            echo -e "\n${RED}ERROR: Translation failed${NC}"
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
        echo "Found $total_files PDF files for Russian translation"
        echo
        
        local processed=0
        local failed=0
        
        for pdf_file in "${pdf_files[@]}"; do
            local filename=$(basename "$pdf_file")
            local output_file="$OUTPUT_DIR/${filename%.pdf}_ru.md"
            
            echo "Processing: $filename -> Russian"
            
            if process_pdf "$pdf_file" "$output_file"; then
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
        echo "=================================================="
        echo "Batch translation completed"
        echo -e "Processed: ${GREEN}$processed${NC} files"
        echo -e "Failed: ${RED}$failed${NC} files"
    fi
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    log "INFO" "Russian translation pipeline completed in ${total_duration}s"
    echo "Total time: ${total_duration}s"
    echo "Log file: $LOG_FILE"
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi