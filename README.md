
# jobs-pipeline

An end-to-end portfolio project that pulls job postings daily from the Reed.co.uk API, cleans them with Python, loads to Azure PostgreSQL, and visualises trends in Power BI/Fabric.

## High-level architecture
```
[Timer Trigger Azure Function]
        │
        ▼
[Reed API] → [Pandas clean/transform] → [Azure PostgreSQL] → [Power BI/Fabric]
```
