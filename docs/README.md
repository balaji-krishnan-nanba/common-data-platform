# Common Data Platform Documentation

Welcome to the Common Data Platform documentation. This framework provides a simple, scalable solution for data ingestion and transformation using Databricks and Azure.

## Quick Start

1. **[Quick Start Guide](getting-started/quick-start.md)** - Get up and running in 15 minutes
2. **[Prerequisites](getting-started/prerequisites.md)** - What you need before starting
3. **[Configuration Guide](configuration/configuration-guide.md)** - How to configure your data sources

## Architecture

- **[Framework Overview](architecture/framework-overview.md)** - Understanding the medallion architecture
- **[Data Flow](architecture/data-flow.md)** - How data moves through the system
- **[Unity Catalog](architecture/unity-catalog.md)** - Governance and security model

## Reference

- **[API Reference](reference/api-reference.md)** - Python API documentation
- **[CLI Commands](reference/api-reference.md#cli-commands)** - Command-line interface reference

## Operations

- **[Operational Guide](operations/operational-guide.md)** - Day-to-day operations
- **[Monitoring Guide](monitoring/monitoring-guide.md)** - Setting up monitoring and alerts
- **[Troubleshooting](monitoring/troubleshooting.md)** - Common issues and solutions

## Advanced

- **[Advanced Configuration](advanced/advanced-configuration.md)** - Custom connectors and complex patterns

## Configuration Structure

All configurations are centralized in the `devops/` directory:

```
devops/
├── config/
│   ├── sources/           # Data source configurations
│   └── transformations/   # Transformation definitions
├── environments/          # Environment-specific settings
├── resources/            # Databricks resources (jobs, clusters)
└── variables/           # Variable definitions
```

This simplified structure eliminates duplication and provides a single source of truth for all configurations.