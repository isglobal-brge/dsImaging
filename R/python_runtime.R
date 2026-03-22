# Module: Python Runtime Management
#
# All dsFlower-framework packages use the same pattern:
#   1. Ensure uv is available (download if needed)
#   2. uv creates a Python venv (downloads Python if needed)
#   3. Use the venv's Python
#
# Zero system dependencies. uv is a single static binary (~30MB)
# that manages Python installations and venvs autonomously.

.dsimaging_runtime <- new.env(parent = emptyenv())

#' Get the Python interpreter for dsImaging
#'
#' Ensures a uv-managed venv exists with Python ready.
#' Lazy: only provisions on first call. Cached for the session.
#'
#' @return Character; absolute path to python3.
#' @keywords internal
.python3 <- function() {
  cached <- .dsimaging_runtime$python3_path
  if (!is.null(cached) && file.exists(cached)) return(cached)

  venv_path <- .dsimaging_venv_path()
  python <- file.path(venv_path, "bin", "python")

  if (!file.exists(python)) {
    uv <- .ensure_uv()
    message("dsImaging: creating Python environment...")
    rc <- system2(uv, c("venv", "--python", "3.11", "--quiet", venv_path),
                  stdout = TRUE, stderr = TRUE)
    if (!file.exists(python))
      stop("Failed to create Python venv at ", venv_path, call. = FALSE)
    message("dsImaging: Python ready at ", python)
  }

  .dsimaging_runtime$python3_path <- python
  python
}

#' Path to dsImaging's own venv
#' @keywords internal
.dsimaging_venv_path <- function() {
  root <- getOption("dsimaging.venv_root",
    getOption("default.dsimaging.venv_root", NULL))
  if (is.null(root)) {
    root <- if (dir.exists("/var/lib/dsimaging")) "/var/lib/dsimaging"
            else file.path(Sys.getenv("HOME", "~"), ".dsimaging")
  }
  file.path(root, "venv")
}

#' Ensure uv is available (find or download)
#'
#' @return Character; path to uv binary.
#' @keywords internal
.ensure_uv <- function() {
  cached <- .dsimaging_runtime$uv_path
  if (!is.null(cached) && file.exists(cached)) return(cached)

  # 1. PATH
  uv <- Sys.which("uv")
  if (nzchar(uv)) { .dsimaging_runtime$uv_path <- uv; return(uv) }

  # 2. Common locations
  home <- Sys.getenv("HOME", "~")
  for (p in c(file.path(home, ".local", "bin", "uv"),
              file.path(home, ".cargo", "bin", "uv"),
              "/usr/local/bin/uv")) {
    if (file.exists(p)) { .dsimaging_runtime$uv_path <- p; return(p) }
  }

  # 3. Download standalone binary
  .install_uv()
}

#' Download uv standalone binary
#' @keywords internal
.install_uv <- function() {
  tools_dir <- file.path(dirname(.dsimaging_venv_path()), "tools")
  dir.create(tools_dir, recursive = TRUE, showWarnings = FALSE)
  uv_path <- file.path(tools_dir, "uv")

  if (file.exists(uv_path)) {
    .dsimaging_runtime$uv_path <- uv_path
    return(uv_path)
  }

  message("dsImaging: downloading uv...")

  sysname <- tolower(Sys.info()[["sysname"]])
  machine <- Sys.info()[["machine"]]
  os <- switch(sysname,
    darwin = "apple-darwin", linux = "unknown-linux-gnu",
    stop("Unsupported OS: ", sysname, ". Install uv manually: https://docs.astral.sh/uv/",
         call. = FALSE))
  arch <- switch(machine,
    x86_64 = "x86_64", amd64 = "x86_64",
    aarch64 = "aarch64", arm64 = "aarch64",
    stop("Unsupported arch: ", machine, call. = FALSE))

  url <- paste0("https://github.com/astral-sh/uv/releases/latest/download/uv-",
                arch, "-", os, ".tar.gz")
  tmp <- tempfile(fileext = ".tar.gz")
  tmp_dir <- tempfile()
  on.exit({ unlink(tmp); unlink(tmp_dir, recursive = TRUE) }, add = TRUE)

  rc <- tryCatch(utils::download.file(url, tmp, mode = "wb", quiet = TRUE),
                  error = function(e) 1L)
  if (!identical(rc, 0L))
    stop("Failed to download uv from ", url, call. = FALSE)

  dir.create(tmp_dir, showWarnings = FALSE)
  utils::untar(tmp, exdir = tmp_dir)
  bins <- list.files(tmp_dir, pattern = "^uv$", recursive = TRUE, full.names = TRUE)
  if (length(bins) == 0)
    stop("uv binary not found in archive.", call. = FALSE)

  file.copy(bins[1], uv_path, overwrite = TRUE)
  Sys.chmod(uv_path, "0755")
  message("dsImaging: uv installed at ", uv_path)
  .dsimaging_runtime$uv_path <- uv_path
  uv_path
}
