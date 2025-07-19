#!/bin/bash

# Databricks Asset Bundle Validation Script
# This script validates DAB configuration without requiring connectivity tests
# Based on user feedback: "dont include connectivity test and sample data test"

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_ROOT/logs/validation.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${1}" | tee -a "$LOG_FILE"
}

# Create logs directory
mkdir -p "$(dirname "$LOG_FILE")"

# Parse command line arguments
TARGET_ENV="${1:-dev}"
VERBOSE="${2:-false}"

log "${BLUE}🔍 Starting Databricks Asset Bundle Validation${NC}"
log "${BLUE}Environment: ${TARGET_ENV}${NC}"
log "${BLUE}Timestamp: $(date)${NC}"

# Validate target environment
if [[ ! "$TARGET_ENV" =~ ^(dev|test|prod)$ ]]; then
    log "${RED}❌ Invalid target environment: $TARGET_ENV${NC}"
    log "${RED}   Valid options: dev, test, prod${NC}"
    exit 1
fi

# Check if databricks CLI is available
if ! command -v databricks &> /dev/null; then
    log "${RED}❌ Databricks CLI not found${NC}"
    log "${RED}   Please install: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh${NC}"
    exit 1
fi

# Validation functions
validate_yaml_syntax() {
    log "${BLUE}📋 Validating YAML syntax...${NC}"
    
    local yaml_files=(
        "databricks.yml"
        "variables/*.yml"
        "resources/**/*.yml"
        "environments/*.yml"
    )
    
    for pattern in "${yaml_files[@]}"; do
        for file in $pattern; do
            if [[ -f "$file" ]]; then
                if python3 -c "import yaml; yaml.safe_load(open('$file'))" 2>/dev/null; then
                    if [[ "$VERBOSE" == "true" ]]; then
                        log "${GREEN}  ✅ $file${NC}"
                    fi
                else
                    log "${RED}❌ YAML syntax error in: $file${NC}"
                    exit 1
                fi
            fi
        done
    done
    
    log "${GREEN}✅ YAML syntax validation passed${NC}"
}

validate_dab_configuration() {
    log "${BLUE}🔧 Validating DAB configuration...${NC}"
    
    # Check if main databricks.yml exists
    if [[ ! -f "databricks.yml" ]]; then
        log "${RED}❌ databricks.yml not found${NC}"
        exit 1
    fi
    
    # Validate bundle configuration using databricks CLI
    if databricks bundle validate --target "$TARGET_ENV" &>/dev/null; then
        log "${GREEN}✅ DAB configuration validation passed${NC}"
    else
        log "${RED}❌ DAB configuration validation failed${NC}"
        log "${YELLOW}Running detailed validation...${NC}"
        databricks bundle validate --target "$TARGET_ENV"
        exit 1
    fi
}

validate_required_variables() {
    log "${BLUE}📝 Validating required variables...${NC}"
    
    local required_vars=(
        "AZURE_TENANT_ID"
    )
    
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        log "${RED}❌ Missing required environment variables:${NC}"
        for var in "${missing_vars[@]}"; do
            log "${RED}   - $var${NC}"
        done
        exit 1
    fi
    
    log "${GREEN}✅ Required variables validation passed${NC}"
}

validate_resource_definitions() {
    log "${BLUE}🏗️ Validating resource definitions...${NC}"
    
    # Check if required resource directories exist
    local resource_dirs=(
        "resources/jobs"
        "resources/clusters"
        "variables"
        "environments"
    )
    
    for dir in "${resource_dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            log "${YELLOW}⚠️ Directory not found: $dir${NC}"
        else
            if [[ "$VERBOSE" == "true" ]]; then
                log "${GREEN}  ✅ $dir${NC}"
            fi
        fi
    done
    
    log "${GREEN}✅ Resource definitions validation passed${NC}"
}

validate_environment_config() {
    log "${BLUE}🌍 Validating environment configuration for: $TARGET_ENV${NC}"
    
    local env_file="environments/${TARGET_ENV}.yml"
    
    if [[ ! -f "$env_file" ]]; then
        log "${RED}❌ Environment configuration not found: $env_file${NC}"
        exit 1
    fi
    
    # Validate environment-specific variables are present
    local env_vars=(
        "AZURE_STORAGE_ACCOUNT_${TARGET_ENV^^}"
        "AZURE_KEY_VAULT_URL_${TARGET_ENV^^}"
    )
    
    for var in "${env_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            log "${YELLOW}⚠️ Environment variable not set: $var${NC}"
        fi
    done
    
    log "${GREEN}✅ Environment configuration validation passed${NC}"
}

# Run validation steps
main() {
    cd "$PROJECT_ROOT"
    
    # Run validation steps
    validate_yaml_syntax
    validate_required_variables
    validate_resource_definitions
    validate_environment_config
    validate_dab_configuration
    
    log "${GREEN}🎉 All validations passed successfully!${NC}"
    log "${GREEN}Bundle is ready for deployment to: $TARGET_ENV${NC}"
}

# Handle errors
trap 'log "${RED}❌ Validation failed at line $LINENO${NC}"; exit 1' ERR

# Run main function
main "$@"