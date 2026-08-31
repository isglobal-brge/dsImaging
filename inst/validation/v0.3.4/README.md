# Executed validation evidence, dsImaging v0.3.4

- `transcript_imaging_fix.txt` — verbatim recorded execution of the
  governed radiomics chain end to end over DSLite with the embedded dsHPC
  backend and a real PyRadiomics 3.0.1 environment: file-backend dataset
  registered from a manifest declaring only `metadata.uri`, ten synthetic
  CT-like NIfTI volumes with masks, client-submitted first batch plus
  server-side drip feed of the remaining batches (10/10 completed, empty
  error field, exact progress accounting), published collection asset with
  derivation hashes and job lineage, and disclosure-gated feature loading
  into a 10x19 table with a federated mean.
- `tests_dsImaging-034_minio.csv` — per-block results of the full test
  suite with the real-MinIO S3 backend test enabled (247 expectations,
  0 failures).
