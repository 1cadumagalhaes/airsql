---
icon: lucide/info
---

# About AirSQL

AirSQL is a decorator-based SQL execution framework for Apache Airflow that simplifies data pipeline development with clean, Pythonic syntax.

## Philosophy

AirSQL was built with a simple philosophy: **make SQL operations in Airflow feel natural**. Instead of writing verbose operator instantiations, AirSQL lets you express your data operations using intuitive Python decorators.

## History

AirSQL emerged from real-world pipeline challenges. As a one-person data engineering team managing multiple databases, environments, and client data integrations, I needed a scalable way to maintain complex data pipelines.

The journey started with Astronomer's astro-sdk. After a year of building on it, the limitations became clear: missing index preservation, broken foreign key constraints, and frequent workarounds. When Astronomer deprecated the package in May 2025, the decision to build something better was obvious.

Since Airflow 3's release, I've been designing an API with clear goals: clean Pythonic code, seamless querying across PostgreSQL and BigQuery, and frictionless data movement between providers. Multi-database support was a core principle from day one, not an afterthought.

What started as a one-week project born from frustration has evolved into a production-ready framework. It now handles multiple data types and export/import formats, supporting nearly every use case we've encountered.

## Key Principles

- **Decorator-first design**: Express data operations as Python functions
- **Type safety**: Full type hints for IDE support and validation
- **Multi-database support**: Works seamlessly with PostgreSQL, BigQuery, and more
- **Airflow native**: Built specifically for Airflow's TaskFlow API

## Roadmap

AirSQL is approaching version 1.0. Looking ahead:

- **v1.x**: Solidify multi-database support, expand provider coverage
- **v2.x**: Decouple core SQL execution from Airflow dependencies, making AirSQL compatible with any orchestrator (including older Airflow versions and alternatives like Dagster, Prefect, etc.), with Airflow operators and decorators as a separate integration layer

**Note**: AirSQL v1.x is built for Airflow 3 and is not compatible with Airflow 2.x. If you need Airflow 2.x support, consider waiting for v2.x or contributing to the effort.

## Maintainer

AirSQL is maintained by **Cadu Magalhaes**, a data engineer passionate about building tools that make data work simpler and more enjoyable.

## Community

- **GitHub**: [1cadumagalhaes/airsql](https://github.com/1cadumagalhaes/airsql)
- **Issues**: Report bugs or request features on [GitHub Issues](https://github.com/1cadumagalhaes/airsql/issues)

## Contributing

Contributions are welcome! Whether it's:

- Bug reports
- Feature requests
- Documentation improvements
- Code contributions

Please visit the [GitHub repository](https://github.com/1cadumagalhaes/airsql) to get started.

## License

AirSQL is released under the MIT License, making it free to use in both personal and commercial projects.