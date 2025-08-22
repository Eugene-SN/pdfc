#!/bin/bash
# Markdown to English Translation Script with Progress Bar
# Translates existing Markdown files from output_md_zh to English using A3B
# Usage: ./translate_md_en.sh [md_file] or ./translate_md_en.sh (for batch processing)

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/output_md_zh"
OUTPUT_DIR="${SCRIPT_DIR}/output_md_en"
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
TARGET_LANGUAGE="english"
MAX_RETRIES=${MAX_RETRIES:-3}
RETRY_DELAY=${RETRY_DELAY:-5}

# Performance tracking
TRANSLATION_START_TIME=""
TOTAL_CHARS_PROCESSED=0
TOTAL_FILES_PROCESSED=0
TOTAL_TOKENS_PROCESSED=0

# Logging setup
LOG_FILE="${LOGS_DIR}/translate_md_en_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$LOGS_DIR"

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Progress bar function with detailed metrics
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
        if [ $elapsed -gt 0 ]; then
            local rate=$((current * 100 / elapsed))
            if [ $rate -gt 0 ]; then
                local remaining_time=$(((total - current) * 100 / rate))
                local eta_mins=$((remaining_time / 60))
                local eta_secs=$((remaining_time % 60))
                eta=" ETA: ${eta_mins}m${eta_secs}s"
            fi
        fi
    fi
    
    printf "\rProgress: ["
    printf "%*s" "$completed" '' | tr ' ' '='
    printf "%*s" "$remaining" '' | tr ' ' '-'
    printf "] %d%% (%d/%d)%s" "$percentage" "$current" "$total" "$eta"
}

# Real-time performance metrics
show_metrics() {
    local current_time=$(date +%s)
    local elapsed=$((current_time - TRANSLATION_START_TIME))
    local chars_per_sec=0
    local tokens_per_sec=0
    local files_per_hour=0
    
    if [ $elapsed -gt 0 ]; then
        chars_per_sec=$((TOTAL_CHARS_PROCESSED / elapsed))
        tokens_per_sec=$((TOTAL_TOKENS_PROCESSED / elapsed))
        files_per_hour=$((TOTAL_FILES_PROCESSED * 3600 / elapsed))
    fi
    
    printf "\nA3B Performance: %d chars/s, %d tokens/s, %d files/h, %ds elapsed" \
           "$chars_per_sec" "$tokens_per_sec" "$files_per_hour" "$elapsed"
}

# Check services health with detailed status
check_services() {
    local services=(
        "$VLLM_SERVER_URL:vLLM A3B Server"
        "$QUALITY_ASSURANCE_URL:Quality Assurance"
    )
    
    echo "Checking A3B service availability..."
    
    for service_info in "${services[@]}"; do
        local url="${service_info%:*}"
        local name="${service_info#*:}"
        local max_attempts=30
        local attempt=1
        
        log "INFO" "Checking $name health at $url"
        
        while [ $attempt -le $max_attempts ]; do
            if curl -s "$url/health" > /dev/null 2>&1; then
                log "INFO" "$name is healthy"
                
                # Additional check for A3B model availability
                if [[ "$name" == *"vLLM"* ]]; then
                    local model_check=$(curl -s "$url/v1/models" | grep -o "Qwen3-30B-A3B-Instruct" || echo "")
                    if [ -n "$model_check" ]; then
                        log "INFO" "A3B model confirmed available"
                    else
                        log "WARN" "A3B model not detected in model list"
                    fi
                fi
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

# Translate markdown using A3B model with enhanced metrics
translate_markdown() {
    local markdown_content="$1"
    local source_file="$2"
    local attempt=1
    
    log "INFO" "Translating markdown: $source_file ($SOURCE_LANGUAGE -> $TARGET_LANGUAGE)"
    log "INFO" "Content length: ${#markdown_content} characters"
    
    # A3B optimized system prompt for English translation
    local system_prompt="You are an expert technical translator using advanced A3B architecture for precise document translation.

TRANSLATION TASK: Chinese → English

CORE PRINCIPLES:
• Preserve ALL technical terms, commands, and code blocks exactly as written
• Maintain perfect markdown formatting structure
• Translate descriptive text to natural, fluent English
• Keep technical terminology in original language when appropriate

CRITICAL PRESERVATION:
• Technical terms: IPMI, BMC, API, REST, JSON, XML, HTTP, CLI, SSH, URL
• Command syntax: ipmitool, curl, file paths, parameters
• Code blocks: \`\`\`bash, \`\`\`json, \`\`\`python etc.
• Formatting: tables, lists, headers, links
• File paths, configuration parameters, error messages

ENGLISH TRANSLATION SPECIFICS:
• Use clear, professional technical English
• Maintain consistent terminology throughout
• Preserve command examples unchanged
• Use standard technical documentation style

OUTPUT REQUIREMENT: Provide ONLY the translated document with no additional commentary."

    local user_prompt="Translate this technical markdown document:

$markdown_content"
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log "INFO" "A3B translation attempt $attempt/$MAX_RETRIES"
        
        local request_start=$(date +%s)
        local request_start_ns=$(date +%s%N)
        
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
    "top_k": 40,
    "stop": ["<|endoftext|>", "<|im_end|>"]
}
EOF
)
        
        local response=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "X-Request-ID: translate_md_en_$(basename "$source_file" .md)_$(date +%s)" \
            -d "$payload" \
            "$VLLM_SERVER_URL/v1/chat/completions" 2>/dev/null)
        
        local request_end=$(date +%s)
        local request_end_ns=$(date +%s%N)
        local request_duration=$((request_end - request_start))
        local request_duration_ms=$(((request_end_ns - request_start_ns) / 1000000))
        
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
                # Extract and log detailed usage statistics
                local usage_stats=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    usage = data.get('usage', {})
    total_tokens = usage.get('total_tokens', 0)
    prompt_tokens = usage.get('prompt_tokens', 0)
    completion_tokens = usage.get('completion_tokens', 0)
    
    # Calculate performance metrics
    tokens_per_sec = total_tokens / $request_duration if $request_duration > 0 else 0
    chars_per_token = ${#markdown_content} / prompt_tokens if prompt_tokens > 0 else 0
    
    print(f\"Tokens - Total: {total_tokens}, Prompt: {prompt_tokens}, Completion: {completion_tokens}\")
    print(f\"Performance - {tokens_per_sec:.1f} tokens/sec, {chars_per_token:.1f} chars/token\")
    print(f\"Timing - Request: {request_duration}s ({request_duration_ms}ms)\")
    print(f\"A3B_STATS:{total_tokens}:{request_duration}\")
except:
    print('Usage stats unavailable')
    print('A3B_STATS:0:0')
" 2>/dev/null)
                
                # Extract A3B stats for aggregation
                local a3b_tokens=$(echo "$usage_stats" | grep "A3B_STATS:" | cut -d: -f2)
                local a3b_time=$(echo "$usage_stats" | grep "A3B_STATS:" | cut -d: -f3)
                
                if [ -n "$a3b_tokens" ] && [ "$a3b_tokens" -gt 0 ]; then
                    TOTAL_TOKENS_PROCESSED=$((TOTAL_TOKENS_PROCESSED + a3b_tokens))
                fi
                
                log "INFO" "A3B translation successful: $(echo "$usage_stats" | head -3 | tr '\n' ', ')"
                
                # Update performance metrics
                TOTAL_CHARS_PROCESSED=$((TOTAL_CHARS_PROCESSED + ${#markdown_content}))
                
                echo "$translated_content"
                return 0
            fi
        fi
        
        log "WARN" "A3B translation attempt $attempt failed"
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "INFO" "Retrying in ${RETRY_DELAY}s..."
            sleep $RETRY_DELAY
        fi
        ((attempt++))
    done
    
    log "ERROR" "A3B translation failed after all attempts for: $source_file"
    return 1
}

# Validate translation quality with enhanced scoring
validate_translation() {
    local original_content="$1"
    local translated_content="$2"
    local source_file="$3"
    
    log "INFO" "Running enhanced quality validation for A3B translation"
    
    local payload=$(cat <<EOF
{
    "document_id": "translate_md_en_$(basename "$source_file" .md)",
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
        "check_fluency": true,
        "markdown_only": true,
        "a3b_validation": true
    }
}
EOF
)
    
    local qa_start=$(date +%s)
    local response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "X-Request-ID: qa_md_en_$(date +%s)" \
        -d "$payload" \
        "$QUALITY_ASSURANCE_URL/validate" 2>/dev/null)
    
    local qa_end=$(date +%s)
    local qa_duration=$((qa_end - qa_start))
    
    if [ $? -eq 0 ] && echo "$response" | grep -q '"success": *true'; then
        log "INFO" "Quality validation completed in ${qa_duration}s"
        
        # Extract comprehensive QA results
        local qa_results=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    qa_score = data.get('overall_score', 0)
    passed = data.get('passed', False)
    corrected = data.get('corrected_document')
    
    # Extract detailed scores
    scores = data.get('validation_scores', {})
    format_score = scores.get('formatting_score', 0)
    content_score = scores.get('content_score', 0)
    tech_score = scores.get('technical_terms_score', 0)
    fluency_score = scores.get('fluency_score', 0)
    completeness_score = scores.get('completeness_score', 0)
    
    print(f\"Overall: {qa_score:.3f} ({'PASS' if passed else 'FAIL'})\")
    print(f\"Scores - Format: {format_score:.3f}, Content: {content_score:.3f}, Tech: {tech_score:.3f}\")
    print(f\"Quality - Fluency: {fluency_score:.3f}, Complete: {completeness_score:.3f}\")
    print(f\"Correction: {'Applied' if corrected else 'None needed'}\")
    
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
        
        local qa_summary=$(echo "$qa_results" | head -4)
        log "INFO" "QA results: $(echo "$qa_summary" | tr '\n' ', ')"
        
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
        log "WARN" "QA validation failed, using original A3B translation"
        echo "$translated_content"
        return 0
    fi
}

# Process single Markdown file with enhanced tracking
process_markdown_file() {
    local md_file="$1"
    local output_file="$2"
    local file_start_time=$(date +%s)
    
    log "INFO" "Processing markdown file: $md_file"
    
    # Read source markdown content with validation
    local markdown_content
    if ! markdown_content=$(cat "$md_file"); then
        log "ERROR" "Failed to read source file: $md_file"
        return 1
    fi
    
    # Validate file size and content
    local file_size=${#markdown_content}
    if [ $file_size -eq 0 ]; then
        log "ERROR" "Empty source file: $md_file"
        return 1
    fi
    
    log "INFO" "File size: $file_size characters"
    
    # Step 1: Translate using A3B model
    echo "Step 1/2: A3B translation to English..."
    local translated_content
    if ! translated_content=$(translate_markdown "$markdown_content" "$md_file"); then
        log "ERROR" "A3B translation failed for: $md_file"
        return 1
    fi
    
    # Step 2: Quality assurance with A3B validation
    echo "Step 2/2: Quality validation and A3B auto-correction..."
    local final_content
    if ! final_content=$(validate_translation "$markdown_content" "$translated_content" "$md_file"); then
        log "ERROR" "QA validation failed for: $md_file"
        return 1
    fi
    
    # Save final result with metadata
    echo "$final_content" > "$output_file"
    
    # Add processing metadata as comment
    local metadata_comment="<!-- Processed by A3B Translation Pipeline v4.1.0 on $(date -Iseconds) -->"
    sed -i "1i\\$metadata_comment" "$output_file"
    
    local file_end_time=$(date +%s)
    local file_duration=$((file_end_time - file_start_time))
    TOTAL_FILES_PROCESSED=$((TOTAL_FILES_PROCESSED + 1))
    
    log "INFO" "Markdown translation completed in ${file_duration}s: $output_file"
    
    return 0
}

# Main function with comprehensive reporting
main() {
    local start_time=$(date +%s)
    TRANSLATION_START_TIME=$start_time
    
    echo -e "${BLUE}Markdown to English Translation v4.1.0 (A3B)${NC}"
    echo "=============================================="
    echo "Source: $INPUT_DIR"
    echo "Output: $OUTPUT_DIR"
    echo "A3B Model: $VLLM_MODEL_ALIAS"
    echo
    
    # Check directories
    if [ ! -d "$INPUT_DIR" ]; then
        log "ERROR" "Source directory not found: $INPUT_DIR"
        exit 1
    fi
    
    mkdir -p "$OUTPUT_DIR"
    
    # Check A3B services
    if ! check_services; then
        log "ERROR" "Required A3B services are not available"
        exit 1
    fi
    
    # Process files
    if [ $# -eq 1 ] && [ -f "$1" ]; then
        # Single file mode
        local md_file="$1"
        local output_file="$OUTPUT_DIR/$(basename "$md_file" .md)_en.md"
        
        log "INFO" "Single file mode: $md_file"
        echo "Processing: $(basename "$md_file") -> English (A3B)"
        
        if process_markdown_file "$md_file" "$output_file"; then
            echo -e "\n${GREEN}SUCCESS: English translation completed${NC}"
            echo "Output: $output_file"
            show_metrics
        else
            echo -e "\n${RED}ERROR: A3B translation failed${NC}"
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
        
        log "INFO" "Batch mode: processing $total_files markdown files with A3B"
        echo "Found $total_files Markdown files for English translation"
        echo
        
        local processed=0
        local failed=0
        
        for md_file in "${md_files[@]}"; do
            local filename=$(basename "$md_file")
            local output_file="$OUTPUT_DIR/${filename%.md}_en.md"
            
            echo "Processing: $filename -> English (A3B)"
            
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
        echo "Batch A3B Translation completed"
        echo -e "Processed: ${GREEN}$processed${NC} files"
        echo -e "Failed: ${RED}$failed${NC} files"
        
        # Comprehensive performance summary
        local end_time=$(date +%s)
        local total_duration=$((end_time - start_time))
        local avg_time_per_file=0
        local avg_chars_per_file=0
        local avg_tokens_per_file=0
        
        if [ $total_files -gt 0 ]; then
            avg_time_per_file=$((total_duration / total_files))
            avg_chars_per_file=$((TOTAL_CHARS_PROCESSED / total_files))
            avg_tokens_per_file=$((TOTAL_TOKENS_PROCESSED / total_files))
        fi
        
        echo
        echo "A3B Performance Summary:"
        echo "  Total time: ${total_duration}s"
        echo "  Average time per file: ${avg_time_per_file}s"
        echo "  Total characters processed: $TOTAL_CHARS_PROCESSED"
        echo "  Total tokens processed: $TOTAL_TOKENS_PROCESSED"
        echo "  Average characters per file: $avg_chars_per_file"
        echo "  Average tokens per file: $avg_tokens_per_file"
        echo "  Processing rate: $((TOTAL_CHARS_PROCESSED / total_duration)) chars/sec"
        echo "  A3B token rate: $((TOTAL_TOKENS_PROCESSED / total_duration)) tokens/sec"
    fi
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    log "INFO" "Markdown to English A3B translation completed in ${total_duration}s"
    echo "Total time: ${total_duration}s"
    echo "Log file: $LOG_FILE"
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi