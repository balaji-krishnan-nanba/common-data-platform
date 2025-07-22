#!/bin/bash

# Databricks Asset Bundle Deployment Script
# Handles validation, deployment, and verification with CI/CD integration
# Enhanced for GitHub Actions with proper error handling and logging

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_ROOT/logs/deploy.log"

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

# Default values
TARGET="dev"
VALIDATE_ONLY=false
FORCE_DEPLOY=false
CI_MODE=false

# Detect CI environment
if [[ "${GITHUB_ACTIONS}" == "true" ]] || [[ "${CI}" == "true" ]]; then
    CI_MODE=true
    FORCE_DEPLOY=true  # Always force deploy in CI
fi

# Function to show usage
usage() {
    log "Usage: $0 [OPTIONS]"
    log ""
    log "Options:"
    log "  -t, --target TARGET       Target environment (dev|test|prod) [default: dev]"
    log "  -v, --validate-only       Only validate, don't deploy"
    log "  -f, --force              Force deployment without confirmation"
    log "  --ci                     Run in CI mode (no interactive prompts)"
    log "  -h, --help               Show this help message"
    log ""
    log "Examples:"
    log "  $0 -t dev                Deploy to development"
    log "  $0 -t prod -f            Force deploy to production"
    log "  $0 -v                    Validate only (no deployment)"
    log "  $0 -t test --ci          Deploy to test in CI mode"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)
            TARGET="$2"
            shift 2
            ;;
        -v|--validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        -f|--force)
            FORCE_DEPLOY=true
            shift
            ;;
        --ci)
            CI_MODE=true
            FORCE_DEPLOY=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
    esac
done

# Validate target
if [[ ! "$TARGET" =~ ^(dev|test|prod)$ ]]; then
    log "${RED}❌ Invalid target: $TARGET${NC}"
    log "${RED}   Valid targets: dev, test, prod${NC}"
    exit 1
fi

log "${BLUE}🚀 Databricks Asset Bundle Deployment${NC}"
log "${BLUE}Target: $TARGET${NC}"
log "${BLUE}Validate Only: $VALIDATE_ONLY${NC}"
log "${BLUE}CI Mode: $CI_MODE${NC}"
log "${BLUE}Timestamp: $(date)${NC}"

# Change to devops directory
cd "$PROJECT_ROOT"

# Step 1: Validate
log ""
log "${BLUE}🔍 Step 1: Validation${NC}"
if ./scripts/validate.sh "$TARGET" quiet; then
    log "${GREEN}✅ Validation completed successfully${NC}"
else
    log "${RED}❌ Validation failed${NC}"
    exit 1
fi

if [[ "$VALIDATE_ONLY" == true ]]; then
    log "${GREEN}✅ Validation completed. Exiting (validate-only mode).${NC}"
    exit 0
fi

# Step 2: Confirmation for production (skip in CI mode)
if [[ "$TARGET" == "prod" && "$FORCE_DEPLOY" != true && "$CI_MODE" != true ]]; then
    log ""
    log "${YELLOW}⚠️ You are about to deploy to PRODUCTION!${NC}"
    log "${YELLOW}   This will affect live data pipelines.${NC}"
    log ""
    read -p "   Are you sure you want to continue? (yes/no): " confirmation
    
    if [[ "$confirmation" != "yes" ]]; then
        log "${RED}❌ Deployment cancelled.${NC}"
        exit 1
    fi
fi

# Step 3: Deploy
log ""
log "${BLUE}📦 Step 2: Deployment${NC}"
log "${BLUE}Deploying bundle to $TARGET environment...${NC}"

# Build deployment command with CI/CD variables if available
DEPLOY_CMD="databricks bundle deploy --target $TARGET"

# Add CI/CD variables if running in CI
if [[ "$CI_MODE" == true ]]; then
    if [[ -n "${GITHUB_REF_NAME}" ]]; then
        DEPLOY_CMD="$DEPLOY_CMD --var git_branch=\"$GITHUB_REF_NAME\""
    fi
    if [[ -n "${GITHUB_SHA}" ]]; then
        DEPLOY_CMD="$DEPLOY_CMD --var git_commit=\"$GITHUB_SHA\""
    fi
    if [[ -n "${GITHUB_SERVER_URL}" && -n "${GITHUB_REPOSITORY}" ]]; then
        DEPLOY_CMD="$DEPLOY_CMD --var git_origin_url=\"$GITHUB_SERVER_URL/$GITHUB_REPOSITORY\""
    fi
    DEPLOY_CMD="$DEPLOY_CMD --var deployment_source=\"github_actions\""
fi

log "${BLUE}Executing: $DEPLOY_CMD${NC}"
eval $DEPLOY_CMD

# Step 4: Verify deployment
log ""
log "${BLUE}✅ Step 3: Verification${NC}"
log "${BLUE}Verifying deployment...${NC}"

# Check if databricks CLI is working
if databricks workspace current &>/dev/null; then
    log "${GREEN}✅ Databricks connection: VERIFIED${NC}"
else
    log "${YELLOW}⚠️ Could not verify Databricks connection${NC}"
fi

# List deployed jobs (if available)
if command -v jq &> /dev/null; then
    JOB_COUNT=$(databricks jobs list --output json 2>/dev/null | jq -r '.jobs | length' || echo "0")
    log "${GREEN}✅ Deployed jobs: $JOB_COUNT${NC}"
    
    if [[ "$JOB_COUNT" -gt 0 ]]; then
        log "${BLUE}📋 Job details:${NC}"
        databricks jobs list --output table 2>/dev/null || log "${YELLOW}⚠️ Could not list jobs${NC}"
    fi
else
    log "${YELLOW}⚠️ jq not available, skipping job count verification${NC}"
fi

# Step 5: Summary
log ""
log "${GREEN}🎉 Deployment completed successfully!${NC}"
log "${GREEN}Environment: $TARGET${NC}"

if [[ "$CI_MODE" == true ]]; then
    log "${GREEN}CI/CD Context:${NC}"
    [[ -n "${GITHUB_REF_NAME}" ]] && log "${GREEN}  Branch: $GITHUB_REF_NAME${NC}"
    [[ -n "${GITHUB_SHA}" ]] && log "${GREEN}  Commit: ${GITHUB_SHA:0:8}${NC}"
    [[ -n "${GITHUB_RUN_ID}" ]] && log "${GREEN}  Run ID: $GITHUB_RUN_ID${NC}"
fi

log ""
log "${BLUE}💡 Next steps:${NC}"
log "${BLUE}   1. Verify resources in Databricks workspace${NC}"
log "${BLUE}   2. Run smoke tests: ./scripts/test.sh $TARGET${NC}"
log "${BLUE}   3. Monitor deployment for any issues${NC}"