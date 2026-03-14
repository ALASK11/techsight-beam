"""Package definition for TechSight Beam pipeline."""

import setuptools

setuptools.setup(
    name="techsight-beam",
    version="0.2.0",
    description="Apache Beam pipeline for Common Crawl script tag analysis",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "apache-beam[gcp]>=2.56.0,<3.0.0",
        "warcio>=1.7.5,<2.0.0",
        "beautifulsoup4>=4.12.0,<5.0.0",
        "lxml>=5.0.0,<6.0.0",
        "requests>=2.31.0,<3.0.0",
        "tldextract>=5.0.0,<6.0.0",
        "pyarrow>=15.0.0,<17.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-cov>=5.0.0",
        ],
    },
    python_requires=">=3.10",
)
