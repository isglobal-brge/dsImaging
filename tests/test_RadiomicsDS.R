# Test script for RadiomicsDS function
# Run with: Rscript tests/test_RadiomicsDS.R

library(dsImaging)
library(dsVault)
library(dsHPC)

vault <- dsVault::DSVaultCollection$new(
  endpoint = "http://localhost:8000",
  collection = "images",
  api_key = "0bbbdae3a5a82c7944f5010083aaa227ba8f337ddd8c8c34c224723cebc608db"
)

hpc <- dsHPC::create_api_config(
  base_url = "http://localhost",
  port = 8001,
  api_key = "lXCXTxsK6JK8aeGSAZkAI8FGLYug8H9u",
  auth_header = "X-API-Key",
  auth_prefix = ""
)

cat("=== RadiomicsDS - Full test ===\n")
result <- RadiomicsDS(
  vault, hpc,
  feature_classes = c("firstorder", "shape"),
  wait = TRUE,
  timeout = 1200,
  polling_interval = 10,
  on_error = "exclude",
  verbose = TRUE
)

cat("\n=== Result ===\n")
cat("Rows:", nrow(result), "\n")
cat("Columns:", ncol(result), "\n")
cat("Feature columns:", sum(grepl("original_", names(result))), "\n")
