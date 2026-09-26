# Databricks notebook source
# MAGIC %md
# MAGIC # Module 03 — Preview the medallion tables
# MAGIC A quick look at Bronze, Silver and Gold before moving to the dashboard, Genie and app.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — raw LinkedIn posts and comments

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.ai_summit.bronze_linkedin_feed ORDER BY created_at DESC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — AI-enriched posts and comments

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.ai_summit.silver_linkedin_enriched ORDER BY created_at DESC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — per-item metrics for BI

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.ai_summit.gold_linkedin_metrics ORDER BY created_at DESC LIMIT 50;
