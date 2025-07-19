#!/bin/bash

# Databricks Asset Bundle Testing Script  
# Runs configuration validation and basic health checks
# Based on user feedback: "dont include connectivity test and sample data test"

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_ROOT/logs/test.log"

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

log "${BLUE}🧪 Starting Databricks Asset Bundle Testing${NC}"
log "${BLUE}Environment: ${TARGET_ENV}${NC}"
log "${BLUE}Timestamp: $(date)${NC}"

# Validate target environment
if [[ ! "$TARGET_ENV" =~ ^(dev|test|prod)$ ]]; then
    log "${RED}❌ Invalid target environment: $TARGET_ENV${NC}"
    log "${RED}   Valid options: dev, test, prod${NC}"
    exit 1
fi

# Change to devops directory
cd "$PROJECT_ROOT"

# Test functions
test_bundle_validation() {
    log "${BLUE}🔍 Test 1: Bundle Validation${NC}"
    
    if ./scripts/validate.sh "$TARGET_ENV" quiet; then
        log "${GREEN}✅ Bundle validation: PASSED${NC}"
        return 0
    else
        log "${RED}❌ Bundle validation: FAILED${NC}"
        return 1
    fi
}

test_configuration_health() {
    log "${BLUE}⚙️ Test 2: Configuration Health Check${NC}"
    
    # Check if databricks CLI is working
    if databricks bundle validate --target "$TARGET_ENV" &>/dev/null; then
        log "${GREEN}✅ DAB configuration: HEALTHY${NC}"
    else
        log "${RED}❌ DAB configuration: UNHEALTHY${NC}"
        return 1
    fi
    
    # Check environment-specific configuration
    local env_file="environments/${TARGET_ENV}.yml"
    if [[ -f "$env_file" ]]; then
        log "${GREEN}✅ Environment config: FOUND${NC}"
    else
        log "${RED}❌ Environment config: MISSING${NC}"
        return 1
    fi
    
    return 0
}

test_resource_definitions() {
    log "${BLUE}🏗️ Test 3: Resource Definition Check${NC}"
    
    local resources_found=0
    
    # Check for job definitions
    if [[ -d "resources/jobs" ]] && [[ $(find resources/jobs -name "*.yml" | wc -l) -gt 0 ]]; then
        log "${GREEN}✅ Job definitions: FOUND${NC}"
        resources_found=$((resources_found + 1))
    else
        log "${YELLOW}⚠️ Job definitions: NOT FOUND${NC}"
    fi
    
    # Check for cluster definitions
    if [[ -d "resources/clusters" ]] && [[ $(find resources/clusters -name "*.yml" | wc -l) -gt 0 ]]; then
        log "${GREEN}✅ Cluster definitions: FOUND${NC}"
        resources_found=$((resources_found + 1))
    else
        log "${YELLOW}⚠️ Cluster definitions: NOT FOUND${NC}"
    fi
    
    # Check for variable definitions
    if [[ -d "variables" ]] && [[ $(find variables -name "*.yml" | wc -l) -gt 0 ]]; then
        log "${GREEN}✅ Variable definitions: FOUND${NC}"
        resources_found=$((resources_found + 1))
    else
        log "${YELLOW}⚠️ Variable definitions: NOT FOUND${NC}"
    fi
    
    if [[ $resources_found -gt 0 ]]; then
        log "${GREEN}✅ Resource definitions: PASSED${NC}"
        return 0
    else
        log "${RED}❌ Resource definitions: FAILED${NC}"
        return 1
    fi
}

test_deployment_readiness() {
    log "${BLUE}🚀 Test 4: Deployment Readiness${NC}"
    
    # Check if bundle can be prepared for deployment
    if databricks bundle validate --target "$TARGET_ENV" &>/dev/null; then
        log "${GREEN}✅ Deployment readiness: READY${NC}"
        return 0
    else
        log "${RED}❌ Deployment readiness: NOT READY${NC}"
        return 1
    fi
}

test_environment_variables() {
    log "${BLUE}📝 Test 5: Environment Variables${NC}"
    
    local required_vars=(
        "AZURE_TENANT_ID"
    )
    
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done
    
    if [[ ${#missing_vars[@]} -eq 0 ]]; then
        log "${GREEN}✅ Environment variables: ALL SET${NC}"
        return 0
    else
        log "${YELLOW}⚠️ Environment variables: SOME MISSING${NC}"
        for var in "${missing_vars[@]}"; do
            log "${YELLOW}   - $var${NC}"
        done
        return 0  # Don't fail on missing env vars, just warn
    fi
}

# Run all tests
main() {
    local failed_tests=0
    local total_tests=5
    
    # Run tests
    test_bundle_validation || failed_tests=$((failed_tests + 1))
    test_configuration_health || failed_tests=$((failed_tests + 1))
    test_resource_definitions || failed_tests=$((failed_tests + 1))
    test_deployment_readiness || failed_tests=$((failed_tests + 1))
    test_environment_variables || failed_tests=$((failed_tests + 1))
    
    # Summary
    log ""
    log "${BLUE}📊 Test Summary${NC}"
    log "${BLUE}Target Environment: ${TARGET_ENV}${NC}"
    
    local passed_tests=$((total_tests - failed_tests))
    log "${GREEN}✅ Passed: ${passed_tests}/${total_tests}${NC}"
    
    if [[ $failed_tests -gt 0 ]]; then
        log "${RED}❌ Failed: ${failed_tests}/${total_tests}${NC}"
        log ""
        log "${YELLOW}⚠️ Some tests failed. Please review the output above.${NC}"
        return 1
    else
        log ""
        log "${GREEN}🎉 All tests passed successfully!${NC}"
        log "${GREEN}Bundle is ready for deployment to: ${TARGET_ENV}${NC}"
        return 0
    fi
}

# Handle errors
trap 'log "${RED}❌ Testing failed at line $LINENO${NC}"; exit 1' ERR

# Run main function
main "$@"