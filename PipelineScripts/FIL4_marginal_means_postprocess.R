#' FIL 4 - Marginal means and postprocessing from FIL 3 GEE models
#'
#' Purpose:
#'   - Read fitted GEE model objects from FIL 3
#'   - Re-read the relevant FIL 2 datasets for reference values and stable typing
#'   - Compute config-driven estimated marginal means (emmeans)
#'   - Compute config-driven contrasts from those emmeans objects
#'   - Export tidy CSV outputs, review flags, summary, and RDS objects
#'
#' Design principles:
#'   - No model fitting happens here; that belongs in FIL 3
#'   - No plotting/report generation happens here; that belongs in FIL 5
#'   - All specs, contrast definitions, and reference-value strategies are config-driven
#'   - Ambiguous inputs are flagged, not silently guessed
#'
#' Usage from another script:
#'   source("FIL4_marginal_means_postprocess.R")
#'   out <- run_fil4("config_with_fil4.yaml")
#'
#' Usage from terminal:
#'   Rscript FIL4_marginal_means_postprocess.R path/to/config.yaml

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(purrr)
  library(yaml)
  library(emmeans)
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

require_cfg <- function(cfg, key) {
  value <- get_cfg(cfg, key, default = NULL)
  if (is.null(value)) {
    stop(sprintf("Missing required config key: '%s'", key), call. = FALSE)
  }
  value
}

coerce_to_character <- function(x) {
  out <- as.character(x)
  out[is.na(x)] <- NA_character_
  out
}

init_review_tbl <- function() {
  tibble(
    stage = character(),
    object_name = character(),
    row_id = integer(),
    column = character(),
    value = character(),
    issue = character(),
    detail = character()
  )
}

append_review_row <- function(review_tbl,
                              stage,
                              object_name,
                              row_id = NA_integer_,
                              column = NA_character_,
                              value = NA_character_,
                              issue,
                              detail = NA_character_) {
  bind_rows(
    review_tbl,
    tibble(
      stage = stage,
      object_name = object_name,
      row_id = row_id,
      column = column,
      value = coerce_to_character(value),
      issue = issue,
      detail = detail
    )
  )
}

infer_fil2_dataset_paths <- function(cfg, project_root) {
  hb_path <- get_cfg(cfg, "fil3.input.gee_hb_ferritin_path", default = NULL)
  side_path <- get_cfg(cfg, "fil3.input.gee_side_effect_path", default = NULL)
  fil2_dir <- file.path(project_root, get_cfg(cfg, "fil2.output.dir", default = "data/interim/fil2"))

  if (is.null(hb_path)) {
    hb_path <- file.path(
      fil2_dir,
      get_cfg(cfg, "fil2.output.gee_hb_ferritin_filename", default = "gee_hb_ferritin.csv")
    )
  } else {
    hb_path <- file.path(project_root, hb_path)
  }

  if (is.null(side_path)) {
    side_path <- file.path(
      fil2_dir,
      get_cfg(cfg, "fil2.output.gee_side_effect_filename", default = "gee_side_effect.csv")
    )
  } else {
    side_path <- file.path(project_root, side_path)
  }

  list(
    gee_hb_ferritin = hb_path,
    gee_side_effect = side_path
  )
}

read_fil2_datasets <- function(cfg, project_root) {
  paths <- infer_fil2_dataset_paths(cfg, project_root)

  datasets <- list(
    gee_hb_ferritin = readr::read_csv(paths$gee_hb_ferritin, show_col_types = FALSE),
    gee_side_effect = readr::read_csv(paths$gee_side_effect, show_col_types = FALSE)
  )

  list(datasets = datasets, paths = paths)
}

coerce_numeric <- function(df, col) {
  if (!(col %in% names(df))) return(df)
  df[[col]] <- suppressWarnings(as.numeric(coerce_to_character(df[[col]])))
  df
}

coerce_factor <- function(df, col, levels_cfg = NULL) {
  if (!(col %in% names(df))) return(df)
  vals <- coerce_to_character(df[[col]])
  vals[vals == ""] <- NA_character_
  df[[col]] <- if (is.null(levels_cfg)) factor(vals) else factor(vals, levels = levels_cfg)
  df
}

prepare_datasets_for_fil4 <- function(datasets, cfg) {
  prep_cfg <- get_cfg(cfg, "fil3.dataset_prep", default = list())
  out <- list()

  for (dataset_name in names(datasets)) {
    df <- datasets[[dataset_name]]
    this_cfg <- prep_cfg[[dataset_name]] %||% list()
    numeric_cols <- unique(unlist(this_cfg$numeric_columns %||% character()))
    factor_cols <- unique(unlist(this_cfg$factor_columns %||% character()))
    factor_levels_cfg <- this_cfg$factor_levels %||% list()

    for (col in numeric_cols) {
      df <- coerce_numeric(df, col)
    }
    for (col in factor_cols) {
      df <- coerce_factor(df, col, factor_levels_cfg[[col]] %||% NULL)
    }

    out[[dataset_name]] <- df
  }

  out
}

load_models <- function(cfg, project_root) {
  rel_path <- get_cfg(
    cfg,
    "fil4.input.combined_models_rds_path",
    default = file.path(
      get_cfg(cfg, "fil3.output.dir", default = "data/interim/fil3"),
      get_cfg(cfg, "fil3.output.combined_models_rds_filename", default = "gee_models.rds")
    )
  )
  combined_path <- file.path(project_root, rel_path)

  models <- readRDS(combined_path)
  list(models = models, combined_path = combined_path)
}

resolve_at_value <- function(dataset, var_name, rule, object_name, review_tbl) {
  if (!(var_name %in% names(dataset))) {
    review_tbl <- append_review_row(
      review_tbl,
      stage = "resolve_at",
      object_name = object_name,
      column = var_name,
      issue = "at_variable_missing",
      detail = "Configured 'at' variable not found in dataset"
    )
    return(list(value = NULL, review_tbl = review_tbl))
  }

  if (is.atomic(rule) && length(rule) == 1 && !is.list(rule)) {
    return(list(value = rule, review_tbl = review_tbl))
  }

  method <- rule$method %||% NULL
  x <- dataset[[var_name]]

  if (is.null(method)) {
    review_tbl <- append_review_row(
      review_tbl,
      stage = "resolve_at",
      object_name = object_name,
      column = var_name,
      issue = "at_method_missing",
      detail = "AT rule must be a scalar or have a 'method' entry"
    )
    return(list(value = NULL, review_tbl = review_tbl))
  }

  if (method == "mean") {
    if (!is.numeric(x)) {
      review_tbl <- append_review_row(
        review_tbl,
        stage = "resolve_at",
        object_name = object_name,
        column = var_name,
        issue = "at_mean_non_numeric",
        detail = "Requested mean() for a non-numeric variable"
      )
      return(list(value = NULL, review_tbl = review_tbl))
    }
    return(list(value = mean(x, na.rm = TRUE), review_tbl = review_tbl))
  }

  if (method == "median") {
    if (!is.numeric(x)) {
      review_tbl <- append_review_row(
        review_tbl,
        stage = "resolve_at",
        object_name = object_name,
        column = var_name,
        issue = "at_median_non_numeric",
        detail = "Requested median() for a non-numeric variable"
      )
      return(list(value = NULL, review_tbl = review_tbl))
    }
    return(list(value = stats::median(x, na.rm = TRUE), review_tbl = review_tbl))
  }

  if (method == "reference_level") {
    if (is.factor(x)) {
      levs <- levels(x)
      if (length(levs) == 0) {
        review_tbl <- append_review_row(review_tbl, "resolve_at", object_name, column = var_name, issue = "no_factor_levels", detail = "Factor has no levels")
        return(list(value = NULL, review_tbl = review_tbl))
      }
      return(list(value = levs[[1]], review_tbl = review_tbl))
    }
    vals <- unique(stats::na.omit(coerce_to_character(x)))
    if (length(vals) == 0) {
      review_tbl <- append_review_row(review_tbl, "resolve_at", object_name, column = var_name, issue = "no_non_missing_values", detail = "Variable has no non-missing values")
      return(list(value = NULL, review_tbl = review_tbl))
    }
    return(list(value = vals[[1]], review_tbl = review_tbl))
  }

  if (method == "value") {
    return(list(value = rule$value %||% NULL, review_tbl = review_tbl))
  }

  review_tbl <- append_review_row(
    review_tbl,
    stage = "resolve_at",
    object_name = object_name,
    column = var_name,
    issue = "unsupported_at_method",
    detail = paste0("Unsupported at method: ", method)
  )
  list(value = NULL, review_tbl = review_tbl)
}

build_at_list <- function(dataset, at_cfg, object_name, review_tbl) {
  if (is.null(at_cfg) || length(at_cfg) == 0) {
    return(list(at = NULL, review_tbl = review_tbl))
  }

  out <- list()
  for (var_name in names(at_cfg)) {
    res <- resolve_at_value(dataset, var_name, at_cfg[[var_name]], object_name, review_tbl)
    review_tbl <- res$review_tbl
    if (!is.null(res$value)) {
      out[[var_name]] <- res$value
    }
  }

  list(at = out, review_tbl = review_tbl)
}

postprocess_emmeans_tbl <- function(tbl, post_cfg = NULL) {
  if (is.null(post_cfg)) return(tbl)

  rename_cfg <- post_cfg$rename %||% list()
  if (length(rename_cfg) > 0) {
    existing_old <- names(rename_cfg)[names(rename_cfg) %in% names(tbl)]
    if (length(existing_old) > 0) {
      names(tbl)[match(existing_old, names(tbl))] <- unname(unlist(rename_cfg[existing_old]))
    }
  }

  num_copy_cfg <- post_cfg$numeric_copy_from_factor %||% list()
  if (length(num_copy_cfg) > 0) {
    for (src_col in names(num_copy_cfg)) {
      dest_col <- num_copy_cfg[[src_col]]
      if (src_col %in% names(tbl)) {
        tbl[[dest_col]] <- suppressWarnings(as.numeric(as.character(tbl[[src_col]])))
      }
    }
  }

  tbl
}

tidy_emmeans_tbl <- function(emm_df, emmeans_name, model_name, dataset_name, specs, type) {
  emm_df %>%
    as_tibble() %>%
    mutate(
      emmeans_name = emmeans_name,
      model = model_name,
      dataset = dataset_name,
      specs = specs,
      response_scale = type,
      .before = 1
    )
}

tidy_contrast_tbl <- function(contrast_df, contrast_name, emmeans_name, model_name, dataset_name, method, adjust, type) {
  contrast_df %>%
    as_tibble() %>%
    mutate(
      contrast_name = contrast_name,
      emmeans_name = emmeans_name,
      model = model_name,
      dataset = dataset_name,
      contrast_method = method,
      adjust = adjust,
      response_scale = type,
      .before = 1
    )
}

compute_one_emmeans <- function(emmeans_name, emm_cfg, models, datasets, cfg, review_tbl) {
  model_name <- require_cfg(list(dummy = emm_cfg), "dummy.model_name")
  specs <- require_cfg(list(dummy = emm_cfg), "dummy.specs")
  type <- get_cfg(list(dummy = emm_cfg), "dummy.type", default = "response")
  at_cfg <- get_cfg(list(dummy = emm_cfg), "dummy.at", default = NULL)
  post_cfg <- get_cfg(list(dummy = emm_cfg), "dummy.postprocess", default = NULL)

  model_cfg <- get_cfg(cfg, paste0("fil3.models.", model_name), default = NULL)
  if (is.null(model_cfg)) {
    review_tbl <- append_review_row(review_tbl, "emmeans", emmeans_name, issue = "model_cfg_missing", detail = paste0("No fil3.models entry found for model: ", model_name))
    return(list(emm = NULL, emmeans_tbl = tibble(), contrasts = list(), review_tbl = review_tbl))
  }

  dataset_key <- model_cfg$dataset_key %||% NULL
  if (is.null(dataset_key) || !(dataset_key %in% names(datasets))) {
    review_tbl <- append_review_row(review_tbl, "emmeans", emmeans_name, issue = "dataset_missing", detail = paste0("Dataset key not available for model: ", model_name))
    return(list(emm = NULL, emmeans_tbl = tibble(), contrasts = list(), review_tbl = review_tbl))
  }

  fit <- models[[model_name]] %||% NULL
  if (is.null(fit)) {
    review_tbl <- append_review_row(review_tbl, "emmeans", emmeans_name, issue = "model_object_missing", detail = paste0("Model object missing or fit failed in FIL 3: ", model_name))
    return(list(emm = NULL, emmeans_tbl = tibble(), contrasts = list(), review_tbl = review_tbl))
  }

  dataset <- datasets[[dataset_key]]
  at_res <- build_at_list(dataset, at_cfg, emmeans_name, review_tbl)
  at_list <- at_res$at
  review_tbl <- at_res$review_tbl

  emm_args <- list(
    object = fit,
    specs = stats::as.formula(specs),
    type = type
  )
  if (!is.null(at_list) && length(at_list) > 0) {
    emm_args$at <- at_list
  }

  emm_obj <- tryCatch(
    do.call(emmeans::emmeans, emm_args),
    error = function(e) e
  )

  if (inherits(emm_obj, "error")) {
    review_tbl <- append_review_row(
      review_tbl,
      stage = "emmeans",
      object_name = emmeans_name,
      issue = "emmeans_failed",
      detail = conditionMessage(emm_obj)
    )
    return(list(emm = NULL, emmeans_tbl = tibble(), contrasts = list(), review_tbl = review_tbl))
  }

  emm_df <- tryCatch(
    summary(emm_obj, infer = c(TRUE, TRUE)) %>% as.data.frame(),
    error = function(e) e
  )

  if (inherits(emm_df, "error")) {
    review_tbl <- append_review_row(
      review_tbl,
      stage = "emmeans",
      object_name = emmeans_name,
      issue = "emmeans_summary_failed",
      detail = conditionMessage(emm_df)
    )
    return(list(emm = emm_obj, emmeans_tbl = tibble(), contrasts = list(), review_tbl = review_tbl))
  }

  emm_df <- postprocess_emmeans_tbl(emm_df, post_cfg)
  emmeans_tbl <- tidy_emmeans_tbl(emm_df, emmeans_name, model_name, dataset_key, specs, type)

  list(
    emm = emm_obj,
    emmeans_tbl = emmeans_tbl,
    contrasts = list(),
    review_tbl = review_tbl
  )
}

compute_contrasts_for_emmeans <- function(emmeans_name, emm_obj, emm_cfg, model_name, dataset_key, type, review_tbl) {
  contrast_cfg <- emm_cfg$contrasts %||% list()
  if (is.null(emm_obj) || length(contrast_cfg) == 0) {
    return(list(contrast_tables = list(), review_tbl = review_tbl))
  }

  contrast_tables <- list()

  for (contrast_name in names(contrast_cfg)) {
    ccfg <- contrast_cfg[[contrast_name]]
    if (!isTRUE(ccfg$enabled %||% TRUE)) next

    method <- ccfg$method %||% "pairwise"
    adjust <- ccfg$adjust %||% "none"
    by <- unlist(ccfg$by %||% character())

    cobj <- tryCatch(
      emmeans::contrast(emm_obj, method = method, by = if (length(by) == 0) NULL else by, adjust = adjust),
      error = function(e) e
    )

    if (inherits(cobj, "error")) {
      review_tbl <- append_review_row(
        review_tbl,
        stage = "contrast",
        object_name = contrast_name,
        issue = "contrast_failed",
        detail = paste0("Emmeans object: ", emmeans_name, " | ", conditionMessage(cobj))
      )
      next
    }

    cdf <- tryCatch(
      summary(cobj, infer = c(TRUE, TRUE)) %>% as.data.frame(),
      error = function(e) e
    )

    if (inherits(cdf, "error")) {
      review_tbl <- append_review_row(
        review_tbl,
        stage = "contrast",
        object_name = contrast_name,
        issue = "contrast_summary_failed",
        detail = paste0("Emmeans object: ", emmeans_name, " | ", conditionMessage(cdf))
      )
      next
    }

    contrast_tables[[contrast_name]] <- tidy_contrast_tbl(
      contrast_df = cdf,
      contrast_name = contrast_name,
      emmeans_name = emmeans_name,
      model_name = model_name,
      dataset_name = dataset_key,
      method = method,
      adjust = adjust,
      type = type
    )
  }

  list(contrast_tables = contrast_tables, review_tbl = review_tbl)
}

compute_all_emmeans <- function(models, datasets, cfg, review_tbl) {
  emm_cfg <- require_cfg(cfg, "fil4.emmeans")
  emm_names <- names(emm_cfg)

  emm_objects <- list()
  emm_tables <- list()
  contrast_tables <- list()

  for (emmeans_name in emm_names) {
    this_cfg <- emm_cfg[[emmeans_name]]
    if (!isTRUE(this_cfg$enabled %||% TRUE)) next

    res <- compute_one_emmeans(emmeans_name, this_cfg, models, datasets, cfg, review_tbl)
    review_tbl <- res$review_tbl
    emm_objects[[emmeans_name]] <- res$emm
    emm_tables[[emmeans_name]] <- res$emmeans_tbl

    model_name <- this_cfg$model_name
    model_cfg <- get_cfg(cfg, paste0("fil3.models.", model_name), default = list())
    dataset_key <- model_cfg$dataset_key %||% NA_character_
    type <- this_cfg$type %||% "response"

    cres <- compute_contrasts_for_emmeans(
      emmeans_name = emmeans_name,
      emm_obj = res$emm,
      emm_cfg = this_cfg,
      model_name = model_name,
      dataset_key = dataset_key,
      type = type,
      review_tbl = review_tbl
    )
    review_tbl <- cres$review_tbl

    if (length(cres$contrast_tables) > 0) {
      contrast_tables <- c(contrast_tables, cres$contrast_tables)
    }
  }

  list(
    emmeans_objects = emm_objects,
    emmeans_table = bind_rows(emm_tables),
    contrast_table = bind_rows(contrast_tables),
    review_tbl = review_tbl
  )
}

build_summary <- function(model_path, fil2_paths, emmeans_tbl, contrast_tbl, review_tbl) {
  tibble(
    metric = c(
      "fil3_combined_models_rds_path",
      "fil2_gee_hb_ferritin_path",
      "fil2_gee_side_effect_path",
      "processed_at_utc",
      "n_emmeans_rows",
      "n_contrast_rows",
      "n_review_flags"
    ),
    value = c(
      model_path,
      fil2_paths$gee_hb_ferritin,
      fil2_paths$gee_side_effect,
      format(Sys.time(), tz = "UTC", usetz = TRUE),
      nrow(emmeans_tbl),
      nrow(contrast_tbl),
      nrow(review_tbl)
    )
  )
}

write_outputs <- function(emm_objects, emmeans_tbl, contrast_tbl, review_tbl, summary_tbl, cfg, project_root) {
  output_dir <- file.path(project_root, get_cfg(cfg, "fil4.output.dir", default = "data/interim/fil4"))
  rds_dir <- file.path(output_dir, get_cfg(cfg, "fil4.output.rds_dir", default = "rds"))

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(rds_dir, recursive = TRUE, showWarnings = FALSE)

  emmeans_path <- file.path(output_dir, get_cfg(cfg, "fil4.output.emmeans_filename", default = "emmeans_results.csv"))
  contrast_path <- file.path(output_dir, get_cfg(cfg, "fil4.output.contrasts_filename", default = "emmeans_contrasts.csv"))
  review_path <- file.path(output_dir, get_cfg(cfg, "fil4.output.review_filename", default = "fil4_review_flags.csv"))
  summary_path <- file.path(output_dir, get_cfg(cfg, "fil4.output.summary_filename", default = "fil4_summary.csv"))
  combined_emm_rds_path <- file.path(output_dir, get_cfg(cfg, "fil4.output.combined_emmeans_rds_filename", default = "emmeans_objects.rds"))

  readr::write_csv(emmeans_tbl, emmeans_path, na = "")
  readr::write_csv(contrast_tbl, contrast_path, na = "")
  readr::write_csv(summary_tbl, summary_path, na = "")

  if (nrow(review_tbl) > 0 || isTRUE(get_cfg(cfg, "fil4.output.write_review_even_if_empty", default = TRUE))) {
    readr::write_csv(review_tbl, review_path, na = "")
  }

  saveRDS(emm_objects, combined_emm_rds_path)

  individual_rds_paths <- list()
  for (nm in names(emm_objects)) {
    if (is.null(emm_objects[[nm]])) next
    this_path <- file.path(rds_dir, paste0(nm, ".rds"))
    saveRDS(emm_objects[[nm]], this_path)
    individual_rds_paths[[nm]] <- this_path
  }

  list(
    emmeans_path = emmeans_path,
    contrast_path = contrast_path,
    review_path = review_path,
    summary_path = summary_path,
    combined_emm_rds_path = combined_emm_rds_path,
    individual_rds_paths = individual_rds_paths
  )
}

run_fil4 <- function(config_path) {
  config_path <- normalizePath(config_path, mustWork = TRUE)
  project_root <- dirname(config_path)
  cfg <- yaml::read_yaml(config_path)
  review_tbl <- init_review_tbl()

  model_res <- load_models(cfg, project_root)
  fil2_res <- read_fil2_datasets(cfg, project_root)
  datasets <- prepare_datasets_for_fil4(fil2_res$datasets, cfg)

  emm_res <- compute_all_emmeans(
    models = model_res$models,
    datasets = datasets,
    cfg = cfg,
    review_tbl = review_tbl
  )

  review_tbl <- emm_res$review_tbl
  summary_tbl <- build_summary(
    model_path = model_res$combined_path,
    fil2_paths = fil2_res$paths,
    emmeans_tbl = emm_res$emmeans_table,
    contrast_tbl = emm_res$contrast_table,
    review_tbl = review_tbl
  )

  output_paths <- write_outputs(
    emm_objects = emm_res$emmeans_objects,
    emmeans_tbl = emm_res$emmeans_table,
    contrast_tbl = emm_res$contrast_table,
    review_tbl = review_tbl,
    summary_tbl = summary_tbl,
    cfg = cfg,
    project_root = project_root
  )

  list(
    emmeans_objects = emm_res$emmeans_objects,
    emmeans = emm_res$emmeans_table,
    contrasts = emm_res$contrast_table,
    review = review_tbl,
    summary = summary_tbl,
    paths = output_paths,
    model_path = model_res$combined_path,
    input_paths = fil2_res$paths
  )
}

if (identical(environment(), globalenv()) && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript FIL4_marginal_means_postprocess.R path/to/config.yaml", call. = FALSE)
  }

  out <- run_fil4(args[[1]])
  message("FIL 4 complete.")
  message("Emmeans CSV:  ", out$paths$emmeans_path)
  message("Contrasts CSV:", out$paths$contrast_path)
  message("Review file:  ", out$paths$review_path)
  message("Summary file: ", out$paths$summary_path)
  message("Combined RDS: ", out$paths$combined_emm_rds_path)
}
