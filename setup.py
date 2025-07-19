"""Setup configuration for the Common Data Platform package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read README file
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Read requirements
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    with open(requirements_file, 'r') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="common-data-platform",
    version="1.0.0",
    description="Modular Data Ingestion Framework for Azure Databricks Lakehouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Data Engineering Team",
    author_email="data-engineering@company.com",
    url="https://github.com/company/common-data-platform",
    
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    
    python_requires=">=3.8",
    install_requires=requirements,
    
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "databricks": [
            "databricks-cli>=0.17.0",
            "databricks-sdk>=0.12.0",
        ]
    },
    
    entry_points={
        "console_scripts": [
            "cdp-provision=scripts.provision_infrastructure:main",
            "cdp-ingest=src.cli:cli",
        ],
    },
    
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    
    keywords="databricks, azure, data-engineering, etl, data-pipeline, lakehouse",
    
    project_urls={
        "Bug Reports": "https://github.com/company/common-data-platform/issues",
        "Source": "https://github.com/company/common-data-platform",
        "Documentation": "https://github.com/company/common-data-platform/wiki",
    },
    
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.sql", "*.md"],
    },
    
    zip_safe=False,
)