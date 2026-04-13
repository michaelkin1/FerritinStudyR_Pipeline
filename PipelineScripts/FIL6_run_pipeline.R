#' FIL 6 - Run the full iron-study pipeline end to end
#'
#' Purpose:
#'   - Orchestrate FIL 1 to FIL 5 in the correct order
#'   - Load each step script safely without auto-running it on source
#'   - Use one shared config file for the whole pipeline
#'   - Record per-step status, timing, output paths, and errors
#'   - Write a pipeline-level summary and run log
#'
#' Expected project structure:
#'   project_root/
#'     config.yaml
#'     PipelineScripts/
#'       FIL1_preprocess_raw_to_clean.R
#'       FIL2_build_analysis_datasets.R
#'       FIL3_fit_gee_models.R
#'       FIL4_marginal_means_postprocess.R
#'       FIL5_final_outputs_report.R
#'       FIL6_run_pipeline.R
#'
#' Usage from another script / console:
#'   source("PipelineScripts/FIL6_run_pipeline.R")
#'   out <- run_fil6("config.yaml")
#'
#' Run from the project root (where config.yaml lives):
#'   out <- run_fil6("config.yaml")
#'
#' Usage from terminal:
#'   Rscript PipelineScripts/FIL6_run_pipeline.R config.yaml

suppressPackageStartupMessages({
  library(yaml)
  library(tibble)
  library(dplyr)
  library(readr)
})

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

get_cfg <- function(cfg, key, default = NULL) {
  parts <- strsplit(key, "\\.")[[1]]
  value <- cfg

  for (part in parts) {
    if (!is.list(value) || is.null(value[[part]])) {
      return(default)
    }
    value <- value[[part]]
  }

  value
}

ensure_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

is_absolute_path <- function(path) {
  grepl("^(?:[A-Za-z]:[/\\\\]|/|\\\\\\\\)", path)
}

resolve_path <- function(path, base_dir) {
  if (is.null(path) || is.na(path) || !nzchar(path)) {
    return(path)
  }

  if (is_absolute_path(path)) {
    return(path)
  }

  file.path(base_dir, path)
}

safe_normalize <- function(path) {
  if (is.null(path) || is.na(path) || !nzchar(path)) {
    return(path)
  }
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

resolve_existing_path <- function(path, search_dirs = character()) {
  if (is.null(path) || is.na(path) || !nzchar(path)) {
    return(path)
  }

  candidates <- character()

  if (is_absolute_path(path)) {
    candidates <- c(candidates, path)
  } else {
    candidates <- c(candidates, path)
    if (length(search_dirs) > 0) {
      candidates <- c(candidates, file.path(search_dirs, path))
    }
  }

  candidates <- unique(candidates)

  for (candidate in candidates) {
    if (file.exists(candidate)) {
      return(safe_normalize(candidate))
    }
  }

  safe_normalize(candidates[[1]])
}

flatten_paths <- function(x, prefix = NULL) {
  if (is.null(x)) {
    return(character())
  }

  if (!is.list(x)) {
    nm <- prefix %||% "value"
    val <- if (length(x) == 0) NA_character_ else paste(as.character(x), collapse = " | ")
    names(val) <- nm
    return(val)
  }

  out <- character()
  nms <- names(x)
  if (is.null(nms)) {
    nms <- paste0("item", seq_along(x))
  }

  for (i in seq_along(x)) {
    this_prefix <- if (is.null(prefix)) nms[[i]] else paste(prefix, nms[[i]], sep = ".")
    out <- c(out, flatten_paths(x[[i]], prefix = this_prefix))
  }

  out
}

init_summary_tbl <- function() {
  tibble(
    step = character(),
    enabled = logical(),
    attempted = logical(),
    status = character(),
    start_time = character(),
    end_time = character(),
    elapsed_seconds = numeric(),
    script_path = character(),
    function_name = character(),
    config_path = character(),
    outputs = character(),
    error_message = character()
  )
}

append_summary_row <- function(summary_tbl,
                               step,
                               enabled,
                               attempted,
                               status,
                               start_time,
                               end_time,
                               elapsed_seconds,
                               script_path,
                               function_name,
                               config_path,
                               outputs = NA_character_,
                               error_message = NA_character_) {
  bind_rows(
    summary_tbl,
    tibble(
      step = step,
      enabled = enabled,
      attempted = attempted,
      status = status,
      start_time = start_time,
      end_time = end_time,
      elapsed_seconds = elapsed_seconds,
      script_path = script_path,
      function_name = function_name,
      config_path = config_path,
      outputs = outputs,
      error_message = error_message
    )
  )
}

build_default_steps <- function(base_dir) {
  scripts_dir <- file.path(base_dir, "PipelineScripts")

  list(
    fil1 = list(
      enabled = TRUE,
      script_path = file.path(scripts_dir, "FIL1_preprocess_raw_to_clean.R"),
      function_name = "run_fil1"
    ),
    fil2 = list(
      enabled = TRUE,
      script_path = file.path(scripts_dir, "FIL2_build_analysis_datasets.R"),
      function_name = "run_fil2"
    ),
    fil3 = list(
      enabled = TRUE,
      script_path = file.path(scripts_dir, "FIL3_fit_gee_models.R"),
      function_name = "run_fil3"
    ),
    fil4 = list(
      enabled = TRUE,
      script_path = file.path(scripts_dir, "FIL4_marginal_means_postprocess.R"),
      function_name = "run_fil4"
    ),
    fil5 = list(
      enabled = TRUE,
      script_path = file.path(scripts_dir, "FIL5_final_outputs_report.R"),
      function_name = "run_fil5"
    )
  )
}

merge_step_config <- function(defaults, overrides, base_dir) {
  steps <- defaults

  if (is.null(overrides)) {
    return(steps)
  }

  for (nm in names(defaults)) {
    ov <- overrides[[nm]]
    if (is.null(ov)) next

    steps[[nm]]$enabled <- ov$enabled %||% defaults[[nm]]$enabled
    steps[[nm]]$script_path <- resolve_path(ov$script_path %||% defaults[[nm]]$script_path, base_dir)
    steps[[nm]]$function_name <- ov$function_name %||% defaults[[nm]]$function_name
    steps[[nm]]$config_path <- resolve_path(ov$config_path %||% NA_character_, base_dir)
  }

  steps
}

load_step_function <- function(script_path, function_name) {
  if (!file.exists(script_path)) {
    stop(sprintf("Step script not found: %s", script_path), call. = FALSE)
  }

  step_env <- new.env(parent = globalenv())
  sys.source(script_path, envir = step_env)

  if (!exists(function_name, envir = step_env, inherits = FALSE)) {
    stop(
      sprintf("Function '%s' not found after loading script: %s", function_name, script_path),
      call. = FALSE
    )
  }

  get(function_name, envir = step_env, inherits = FALSE)
}

collapse_output_paths <- function(result_obj) {
  if (is.null(result_obj) || is.null(result_obj$paths)) {
    return(NA_character_)
  }

  flat <- flatten_paths(result_obj$paths)
  if (length(flat) == 0) {
    return(NA_character_)
  }

  paste(sprintf("%s=%s", names(flat), flat), collapse = " ; ")
}

write_pipeline_outputs <- function(summary_tbl, log_lines, cfg, base_dir) {
  out_dir <- resolve_path(get_cfg(cfg, "fil6.output.dir", default = "outputs/fil6"), base_dir)
  out_dir <- ensure_dir(out_dir)

  summary_path <- file.path(
    out_dir,
    get_cfg(cfg, "fil6.output.summary_filename", default = "pipeline_run_summary.csv")
  )
  log_path <- file.path(
    out_dir,
    get_cfg(cfg, "fil6.output.log_filename", default = "pipeline_run_log.txt")
  )

  readr::write_csv(summary_tbl, summary_path, na = "")
  writeLines(log_lines, con = log_path, useBytes = TRUE)

  list(
    output_dir = out_dir,
    summary_path = summary_path,
    log_path = log_path
  )
}

run_fil6 <- function(config_path = "config.yaml") {
  script_dir <- get0(".FIL6_SCRIPT_DIR", ifnotfound = NA_character_, inherits = TRUE)
  search_dirs <- unique(stats::na.omit(c(getwd(), script_dir)))

  config_path <- resolve_existing_path(config_path, search_dirs = search_dirs)

  if (!file.exists(config_path)) {
    stop(
      sprintf(
        "Config file not found: %s. Tried relative to: %s",
        config_path,
        paste(search_dirs, collapse = " | ")
      ),
      call. = FALSE
    )
  }

  config_path <- safe_normalize(config_path)
  cfg <- yaml::read_yaml(config_path)
  base_dir <- dirname(config_path)

  start_step <- as.integer(get_cfg(cfg, "fil6.run.start_step", default = 1L))
  end_step <- as.integer(get_cfg(cfg, "fil6.run.end_step", default = 5L))
  stop_on_error <- isTRUE(get_cfg(cfg, "fil6.run.stop_on_error", default = TRUE))

  if (is.na(start_step) || is.na(end_step) || start_step < 1L || end_step > 5L || start_step > end_step) {
    stop("Invalid fil6.run.start_step / fil6.run.end_step configuration.", call. = FALSE)
  }

  step_defaults <- build_default_steps(base_dir)
  steps <- merge_step_config(step_defaults, get_cfg(cfg, "fil6.steps", default = NULL), base_dir)

  all_step_names <- names(steps)
  selected_step_names <- all_step_names[seq.int(start_step, end_step)]

  summary_tbl <- init_summary_tbl()
  log_lines <- c(
    paste0("FIL 6 pipeline run started: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    paste0("Config: ", config_path),
    paste0("Base dir: ", base_dir),
    paste0("Step range: ", start_step, " -> ", end_step),
    paste0("stop_on_error: ", stop_on_error),
    ""
  )

  results <- list()
  pipeline_error <- NULL

  for (step_name in all_step_names) {
    step_cfg <- steps[[step_name]]
    enabled <- isTRUE(step_cfg$enabled)
    attempted <- step_name %in% selected_step_names && enabled
    this_config_path <- safe_normalize(if (is.null(step_cfg$config_path) || is.na(step_cfg$config_path)) config_path else step_cfg$config_path)
    this_script_path <- safe_normalize(step_cfg$script_path)
    this_function_name <- step_cfg$function_name

    if (!attempted) {
      reason <- if (!enabled) "disabled" else "outside_selected_range"
      summary_tbl <- append_summary_row(
        summary_tbl = summary_tbl,
        step = step_name,
        enabled = enabled,
        attempted = FALSE,
        status = reason,
        start_time = NA_character_,
        end_time = NA_character_,
        elapsed_seconds = NA_real_,
        script_path = this_script_path,
        function_name = this_function_name,
        config_path = this_config_path,
        outputs = NA_character_,
        error_message = NA_character_
      )
      log_lines <- c(log_lines, paste0("[", step_name, "] skipped: ", reason))
      next
    }

    started_at <- Sys.time()
    log_lines <- c(
      log_lines,
      paste0("[", step_name, "] started at ", format(started_at, "%Y-%m-%d %H:%M:%S")),
      paste0("  script: ", this_script_path),
      paste0("  function: ", this_function_name),
      paste0("  config: ", this_config_path)
    )

    step_result <- tryCatch(
      {
        step_fun <- load_step_function(this_script_path, this_function_name)
        step_fun(this_config_path)
      },
      error = function(e) e
    )

    ended_at <- Sys.time()
    elapsed <- as.numeric(difftime(ended_at, started_at, units = "secs"))

    if (inherits(step_result, "error")) {
      err_msg <- conditionMessage(step_result)
      pipeline_error <- err_msg

      summary_tbl <- append_summary_row(
        summary_tbl = summary_tbl,
        step = step_name,
        enabled = enabled,
        attempted = TRUE,
        status = "failed",
        start_time = format(started_at, "%Y-%m-%d %H:%M:%S"),
        end_time = format(ended_at, "%Y-%m-%d %H:%M:%S"),
        elapsed_seconds = elapsed,
        script_path = this_script_path,
        function_name = this_function_name,
        config_path = this_config_path,
        outputs = NA_character_,
        error_message = err_msg
      )

      log_lines <- c(
        log_lines,
        paste0("[", step_name, "] FAILED after ", round(elapsed, 2), " sec"),
        paste0("  error: ", err_msg),
        ""
      )

      if (stop_on_error) {
        break
      } else {
        next
      }
    }

    results[[step_name]] <- step_result
    outputs_string <- collapse_output_paths(step_result)

    summary_tbl <- append_summary_row(
      summary_tbl = summary_tbl,
      step = step_name,
      enabled = enabled,
      attempted = TRUE,
      status = "completed",
      start_time = format(started_at, "%Y-%m-%d %H:%M:%S"),
      end_time = format(ended_at, "%Y-%m-%d %H:%M:%S"),
      elapsed_seconds = elapsed,
      script_path = this_script_path,
      function_name = this_function_name,
      config_path = this_config_path,
      outputs = outputs_string,
      error_message = NA_character_
    )

    log_lines <- c(
      log_lines,
      paste0("[", step_name, "] completed in ", round(elapsed, 2), " sec"),
      if (!is.na(outputs_string)) paste0("  outputs: ", outputs_string) else "  outputs: <none>",
      ""
    )
  }

  completed_n <- sum(summary_tbl$status == "completed", na.rm = TRUE)
  failed_n <- sum(summary_tbl$status == "failed", na.rm = TRUE)
  skipped_n <- sum(summary_tbl$status %in% c("disabled", "outside_selected_range"), na.rm = TRUE)

  log_lines <- c(
    log_lines,
    "----------------------------------------",
    paste0("FIL 6 pipeline run finished: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    paste0("Completed steps: ", completed_n),
    paste0("Failed steps: ", failed_n),
    paste0("Skipped steps: ", skipped_n),
    if (!is.null(pipeline_error)) paste0("Pipeline error: ", pipeline_error) else "Pipeline error: <none>"
  )

  output_paths <- write_pipeline_outputs(summary_tbl, log_lines, cfg, base_dir)

  out <- list(
    step_results = results,
    summary = summary_tbl,
    log_lines = log_lines,
    paths = output_paths,
    config_path = config_path,
    pipeline_error = pipeline_error
  )

  if (!is.null(pipeline_error) && stop_on_error) {
    stop(
      paste0(
        "FIL 6 stopped because one of the steps failed. ",
        "See summary: ", output_paths$summary_path,
        " | log: ", output_paths$log_path,
        " | error: ", pipeline_error
      ),
      call. = FALSE
    )
  }

  out
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    args <- "config.yaml"
  }

  out <- run_fil6(args[[1]])
  message("FIL 6 finished.")
  message("Summary file: ", out$paths$summary_path)
  message("Log file:     ", out$paths$log_path)
}
