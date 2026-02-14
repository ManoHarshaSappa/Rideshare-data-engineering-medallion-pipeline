# ------------------------------------------------------------
# utils package initializer
# ------------------------------------------------------------
# This file marks the 'utils' directory as a Python package.
#
# Even though this file does not contain executable code,
# it is important for the project structure because it allows
# other modules (like bronze_ingestion.py and silver_transformation.py)
# to import functions from the utils package using statements like:
#
#     from utils.custom_utils import some_function
#
# In this project, we do not need any package-level initialization
# (such as variables, configurations, or automatic imports),
# so this file is intentionally kept empty except for documentation comments.
#
# Keeping it ensures:
# - Clean modular structure
# - Proper package imports
# - Compatibility across environments (local, EMR, Databricks, etc.)
# ------------------------------------------------------------
