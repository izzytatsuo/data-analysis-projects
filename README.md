# Data Analysis Projects

This repository contains organized data analysis projects including parquet analysis, shipment timeline analysis, and AWS integrations.

## 🚀 Quick Start

```bash
git clone git@github.com:izzytatsuo/data-analysis-projects.git
cd data-analysis-projects
```

## 📁 Project Structure

### `/aws-bedrock/`
AWS Bedrock knowledge base integration for Redshift querying:
- Natural language SQL query interface
- Knowledge base ID: `RD2ZPKUAUT`
- Configuration and usage guides

### `/data-analysis/`
Core analysis projects:
- **`parquet-analysis/`**: Tools for analyzing parquet files and schemas
- **`shipment-timeline/`**: Container planning and shipment timeline analysis

### `/sql-work/`
SQL schemas and queries:
- ETL job definitions and schemas
- Consolidated database schemas
- Analysis queries for logistics data

### `/notebooks/`
Jupyter notebooks for interactive analysis:
- SQL testing and exploration
- Data visualization and analysis
- Working analysis prototypes

### `/claude-setup/`
Development environment setup:
- Claude Code configuration for WSL
- Remote development setup guides
- SSH and connection troubleshooting

### `/scripts/`
Utility scripts and tools:
- Python data processing utilities
- File operation tools
- Reusable analysis components

## 🔧 Key Technologies

- **AWS Bedrock**: Natural language to SQL translation
- **Redshift**: Data warehousing and analytics  
- **Parquet**: Columnar data format analysis
- **Python**: Data processing and analysis
- **Jupyter**: Interactive analysis environment
- **Claude Code**: AI-assisted development

## 💾 Data Storage Strategy

Large data files (>100MB) are stored separately:
- **S3 buckets** for production parquet files
- **Local file store** for development and testing
- **Repository** contains schemas, scripts, and documentation

## 🎯 Main Use Cases

1. **Logistics Data Analysis**: Amazon shipment and delivery analytics
2. **ETL Pipeline Development**: Data transformation and loading
3. **Interactive Analysis**: Jupyter-based data exploration
4. **Knowledge Base Queries**: Natural language data querying

## 🔗 Related Resources

- [AWS Bedrock Documentation](aws-bedrock/CLAUDE.md)
- [Claude Setup Guide](claude-setup/claude-connection-guide.md)
- [Parquet Analysis Tools](data-analysis/parquet-analysis/)
- [Shipment Timeline Analysis](data-analysis/shipment-timeline/)

---

**Last Updated**: July 2025  
**Repository**: https://github.com/izzytatsuo/data-analysis-projects