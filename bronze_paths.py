"""
bronze_paths.py (project root)

Single source of truth for the Bronze S3 key prefixes, shared by both the
crawler (I/O layer — writes these keys) and the parser (pure core — filters
files by these keys) packages.

Deliberately dependency-free (no dotenv, no env reads, no I/O of any kind)
so pure core modules can import it safely — including
parser/bronze_to_silver_core.py, which is reused inside Spark closures and
must stay free of config/I/O imports.
"""

DATASET_PREFIX = "bronze/dataset/"
WEB_PREFIX = "bronze/web/"
