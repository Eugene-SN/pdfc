#!/bin/bash
# Markdown to Russian Translation Script with Progress Bar
# Translates existing Markdown files from output_md_zh to Russian using A3B
# Usage: ./translate_md_ru.sh [md_file] or ./translate_md_ru.sh (for batch processing)

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/output_md_zh"
OUTPUT_DIR="${SCRIPT_DIR}/output_md_ru"
LOGS_DIR="${SCRIPT_DIR}/logs"
CONFIG_FILE="${SCRIPT_DIR}/.env"

# Colors for output (no emojis as requested)
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
VLLM_SERVER_URL=${VLLM_SERVER_URL:-"http://localhost:8000"}
QUALITY_ASSURANCE_URL=${QUALITY_ASSURANCE_URL:-"http://localhost:8002"}

# A3B Model Configuration
VLLM_MODEL_ALIAS=${VLLM_MODEL_ALIAS:-"Qwen3-30B-A3B-Instruct"}
A3B_TEMPERATURE=${A3B_TEMPERATURE:-"0.1"}
A3B_MAX_TOKENS=${A3B_MAX_TOKENS:-"6144"}
A3B_TOP_P=${A3B_TOP_P:-"0.8"}

# Translation parameters
SOURCE_LANGUAGE="chinese"
TARGET_LANGUAGE="russian"
MAX_RETRIES=${MAX_RETRIES:-3}
RETRY_DELAY=${RETRY_DELAY:-5}

# Performance tracking
TRANSLATION_START_TIME=""
TOTAL_CHARS_PROCESSED=0
TOTAL_FILES_PROCESSED=0

# Logging setup
LOG_FILE="${LOGS_DIR}/translate_md_ru_$(date +%Y%m%d_%H%M%S).log"
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
    local eta=""
    
    # Calculate ETA if we have timing data
    if [ -n "$TRANSLATION_START_TIME" ] && [ $current -gt 0 ]; then
        local elapsed=$(($(date +%s) - TRANSLATION_START_TIME))
        local rate=$((current * 100 / elapsed))
        if [ $rate -gt 0 ]; then
            local remaining_time=$(((total - current) * 100 / rate))
            local eta_mins=$((remaining_time / 60))
            local eta_secs=$((remaining_time % 60))
            eta=" ETA: ${eta_mins}m${eta_secs}s"
        fi
    fi
    
    printf "\rProgress: ["
    printf "%*s" "$completed" '' | tr ' ' '='
    printf "%*s" "$remaining" '' | tr ' ' '-'
    printf "] %d%% (%d/%d)%s" "$percentage" "$current" "$total" "$eta"
}

# Performance metrics display
show_metrics() {
    local current_time=$(date +%s)
    local elapsed=$((current_time - TRANSLATION_START_TIME))
    local chars_per_sec=0
    local files_per_min=0
    
    if [ $elapsed -gt 0 ]; then
        chars_per_sec=$((TOTAL_CHARS_PROCESSED / elapsed))
        files_per_min=$((TOTAL_FILES_PROCESSED * 60 / elapsed))
    fi
    
    printf "\nPerformance Metrics: %d chars/sec, %d files/min, %ds elapsed" \
           "$chars_per_sec" "$files_per_min" "$elapsed"
}

# Check services health
check_services() {
    local services=(
        "$VLLM_SERVER_URL:vLLM A3B Server"
        "$QUALITY_ASSURANCE_URL:Quality Assurance"
    )
    
    echo "Checking service availability..."
    
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
            
            printf "\rChecking %s... [%d/%d]" "$name" "$attempt" "$max_attempts"
            sleep 2
            ((attempt++))
        done
    done
    
    echo
    return 0
}

# Translate markdown using A3B model
translate_markdown() {
    local markdown_content="$1"
    local source_file="$2"
    local attempt=1
    
    log "INFO" "Translating markdown: $source_file ($SOURCE_LANGUAGE -> $TARGET_LANGUAGE)"
    log "INFO" "Content length: ${#markdown_content} characters"
    
    # A3B optimized system prompt for Russian translation
    local system_prompt="You are an expert technical translator using advanced A3B architecture for precise document translation.

TRANSLATION TASK: Chinese → Russian

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
• File paths, configuration parameters, error messages

RUSSIAN TRANSLATION SPECIFICS:
• Use appropriate Russian technical terminology
• Maintain formal technical writing style
• Preserve command examples unchanged
• Keep English technical abbreviations where standard

OUTPUT REQUIREMENT: Provide ONLY the translated document with no additional commentary."

    local user_prompt="Translate this technical markdown document:

$markdown_content"
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log "INFO" "Translation attempt $attempt/$MAX_RETRIES using A3B model"
        
        local request_start=$(date +%s)
        
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
        
        local response=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "X-Request-ID: translate_md_ru_$(basename "$source_file" .md)_$(date +%s)" \
            -d "$payload" \
            "$VLLM_SERVER_URL/v1/chat/completions" 2>/dev/null)
        
        local request_end=$(date +%s)
        local request_duration=$((request_end - request_start))
        
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
                # Extract and log usage statistics
                local usage_info=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    usage = data.get('usage', {})
    total_tokens = usage.get('total_tokens', 0)
    prompt_tokens = usage.get('prompt_tokens', 0)
    completion_tokens = usage.get('completion_tokens', 0)
    
    # Calculate tokens per second
    tokens_per_sec = total_tokens / $request_duration if $request_duration > 0 else 0
    
    print(f\"Total: {total_tokens}, Prompt: {prompt_tokens}, Completion: {completion_tokens}\")
    print(f\"Speed: {tokens_per_sec:.1f} tokens/sec, Duration: {request_duration}s\")
except:
    print('Usage stats unavailable')
" 2>/dev/null)
                
                log "INFO" "A3B translation successful: $usage_info"
                
                # Update performance metrics
                TOTAL_CHARS_PROCESSED=$((TOTAL_CHARS_PROCESSED + ${#markdown_content}))
                
                echo "$translated_content"
                return 0
            fi
        fi
        
        log "WARN" "Translation attempt $attempt failed"
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "INFO" "Retrying in ${RETRY_DELAY}s..."
            sleep $RETRY_DELAY
        fi
        ((attempt++))
    done
    
    log "ERROR" "Translation failed after all attempts for: $source_file"
    return 1
}

# Validate translation quality
validate_translation() {
    local original_content="$1"
    local translated_content="$2"
    local source_file="$3"
    
    log "INFO" "Running quality validation for translated document"
    
    local payload=$(cat <<EOF
{
    "document_id": "translate_md_ru_$(basename "$source_file" .md)",
    "original_markdown": "$original_content",
    "translated_markdown": "$translated_content",
    "source_language": "$SOURCE_LANGUAGE",
    "target_language": "$TARGET_LANGUAGE",
    "validation_options": {
        "enable_auto_correction": true,
        "qa_threshold": 0.85,
        "check_formatting": true,
        "check_technical_terms": true,
        "check_completeness": true,
        "markdown_only": true
    }
}
EOF
)
    
    local qa_start=$(date +%s)
    local response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "X-Request-ID: qa_md_ru_$(date +%s)" \
        -d "$payload" \
        "$QUALITY_ASSURANCE_URL/validate" 2>/dev/null)
    
    local qa_end=$(date +%s)
    local qa_duration=$((qa_end - qa_start))
    
    if [ $? -eq 0 ] && echo "$response" | grep -q '"success": *true'; then
        log "INFO" "Quality validation completed in ${qa_duration}s"
        
        # Extract QA results and metrics
        local qa_results=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    qa_score = data.get('overall_score', 0)
    passed = data.get('passed', False)
    corrected = data.get('corrected_document')
    
    # Extract individual scores
    scores = data.get('validation_scores', {})
    format_score = scores.get('formatting_score', 0)
    content_score = scores.get('content_score', 0)
    tech_score = scores.get('technical_terms_score', 0)
    
    print(f\"Overall: {qa_score:.3f}, Format: {format_score:.3f}, Content: {content_score:.3f}, Tech: {tech_score:.3f}\")
    print(f\"Passed: {passed}, Auto-corrected: {'Yes' if corrected else 'No'}\")
    
    # Return corrected version if available, otherwise original translation
    if corrected:
        print('CORRECTED_CONTENT:')
        print(corrected)
    else:
        print('ORIGINAL_TRANSLATION:')
        print('$translated_content')
except Exception as e:
    print('QA validation error')
    print('ORIGINAL_TRANSLATION:')
    print('$translated_content')
" 2>/dev/null)
        
        local qa_summary=$(echo "$qa_results" | head -2)
        log "INFO" "QA results: $qa_summary"
        
        # Extract final content
        local final_content
        if echo "$qa_results" | grep -q "CORRECTED_CONTENT:"; then
            final_content=$(echo "$qa_results" | sed -n '/^CORRECTED_CONTENT:/,${/^CORRECTED_CONTENT:/d; p;}')
        else
            final_content=$(echo "$qa_results" | sed -n '/^ORIGINAL_TRANSLATION:/,${/^ORIGINAL_TRANSLATION:/d; p;}')
        fi
        
        echo "$final_content"
        return 0
    else
        log "WARN" "QA validation failed, using original translation"
        echo "$translated_content"
        return 0
    fi
}

# Process single Markdown file
process_markdown_file() {
    local md_file="$1"
    local output_file="$2"
    local file_start_time=$(date +%s)
    
    log "INFO" "Processing markdown file: $md_file"
    
    # Read source markdown content
    local markdown_content
    if ! markdown_content=$(cat "$md_file"); then
        log "ERROR" "Failed to read source file: $md_file"
        return 1
    fi
    
    # Step 1: Translate using A3B model
    echo "Step 1/2: Translating to Russian using A3B model..."
    local translated_content
    if ! translated_content=$(translate_markdown "$markdown_content" "$md_file"); then
        log "ERROR" "Translation failed for: $md_file"
        return 1
    fi
    
    # Step 2: Quality assurance
    echo "Step 2/2: Quality validation and auto-correction..."
    local final_content
    if ! final_content=$(validate_translation "$markdown_content" "$translated_content" "$md_file"); then
        log "ERROR" "QA validation failed for: $md_file"
        return 1
    fi
    
    # Save final result
    echo "$final_content" > "$output_file"
    
    local file_end_time=$(date +%s)
    local file_duration=$((file_end_time - file_start_time))
    TOTAL_FILES_PROCESSED=$((TOTAL_FILES_PROCESSED + 1))
    
    log "INFO" "Markdown translation completed in ${file_duration}s: $output_file"
    
    return 0
}

# Main function
main() {
    local start_time=$(date +%s)
    TRANSLATION_START_TIME=$start_time
    
    echo -e "${BLUE}Markdown to Russian Translation v4.1.0 (A3B)${NC}"
    echo "=============================================="
    echo "Source: $INPUT_DIR"
    echo "Output: $OUTPUT_DIR"
    echo
    
    # Check directories
    if [ ! -d "$INPUT_DIR" ]; then
        log "ERROR" "Source directory not found: $INPUT_DIR"
        exit 1
    fi
    
    mkdir -p "$OUTPUT_DIR"
    
    # Check services
    if ! check_services; then
        log "ERROR" "Required services are not available"
        exit 1
    fi
    
    # Process files
    if [ $# -eq 1 ] && [ -f "$1" ]; then
        # Single file mode
        local md_file="$1"
        local output_file="$OUTPUT_DIR/$(basename "$md_file" .md)_ru.md"
        
        log "INFO" "Single file mode: $md_file"
        echo "Processing: $(basename "$md_file") -> Russian"
        
        if process_markdown_file "$md_file" "$output_file"; then
            echo -e "\n${GREEN}SUCCESS: Russian translation completed${NC}"
            echo "Output: $output_file"
            show_metrics
        else
            echo -e "\n${RED}ERROR: Translation failed${NC}"
            exit 1
        fi
    else
        # Batch mode
        local md_files=()
        while IFS= read -r -d '' file; do
            md_files+=("$file")
        done < <(find "$INPUT_DIR" -name "*.md" -type f -print0)
        
        local total_files=${#md_files[@]}
        
        if [ $total_files -eq 0 ]; then
            log "WARN" "No Markdown files found in $INPUT_DIR"
            echo "No Markdown files found for processing"
            exit 0
        fi
        
        log "INFO" "Batch mode: processing $total_files markdown files"
        echo "Found $total_files Markdown files for Russian translation"
        echo
        
        local processed=0
        local failed=0
        
        for md_file in "${md_files[@]}"; do
            local filename=$(basename "$md_file")
            local output_file="$OUTPUT_DIR/${filename%.md}_ru.md"
            
            echo "Processing: $filename -> Russian"
            
            if process_markdown_file "$md_file" "$output_file"; then
                ((processed++))
                echo -e "Status: ${GREEN}SUCCESS${NC}"
            else
                ((failed++))
                echo -e "Status: ${RED}FAILED${NC}"
            fi
            
            show_progress $((processed + failed)) $total_files
            show_metrics
            echo
        done
        
        echo
        echo "=============================================="
        echo "Batch Markdown translation completed"
        echo -e "Processed: ${GREEN}$processed${NC} files"
        echo -e "Failed: ${RED}$failed${NC} files"
        
        # Final performance summary
        local end_time=$(date +%s)
        local total_duration=$((end_time - start_time))
        local avg_time_per_file=$((total_duration / total_files))
        local avg_chars_per_file=$((TOTAL_CHARS_PROCESSED / total_files))
        
        echo
        echo "Performance Summary:"
        echo "  Total time: ${total_duration}s"
        echo "  Average time per file: ${avg_time_per_file}s"
        echo "  Total characters processed: $TOTAL_CHARS_PROCESSED"
        echo "  Average characters per file: $avg_chars_per_file"
        echo "  Processing rate: $((TOTAL_CHARS_PROCESSED / total_duration)) chars/sec"
    fi
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    log "INFO" "Markdown to Russian translation completed in ${total_duration}s"
    echo "Total time: ${total_duration}s"
    echo "Log file: $LOG_FILE"
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi