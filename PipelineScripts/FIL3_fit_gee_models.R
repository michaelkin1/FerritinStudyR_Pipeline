#' FIL 3 - Fit GEE models from FIL 2 analysis datasets
#'
#' Purpose:
#'   - Read the model-ready datasets exported by FIL 2
#'   - Re-apply config-driven types and factor reference levels
#'   - Fit one or more GEE models in a deterministic, pipeline-friendly way
#'   - Export coefficient tables, model metadata, review flags, and model objects
#'
#' Design principles:
#'   - No raw-data cleaning happens here; that belongs in FIL 1
#'   - No dataset construction/filtering happens here; that belongs in FIL 2
#'   - No marginal means or plotting happens here; that belongs in FIL 4+
#'   - All model formulas, families, paths, and factor levels are config-driven
#'
#' Usage from another script:
#'   source("FIL3_fit_gee_models.R")
#'   out <- run_fil3("config_with_fil3.yaml")
#'
#' Usage from terminal:
#'   Rscript FIL3_fit_gee_models.R path/to/config.yaml

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(purrr)
  library(yaml)
  library(geepack)
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
    dataset = character(),
    model = character(),
    row_id = integer(),
    column = character(),
    value = character(),
    issue = character(),
    detail = character()
  )
}

append_review_flags <- function(review_tbl,
                                dataset,
                                model = NA_character_,
                                row_ids,
                                column,
                                value,
                                issue,
                                detail = NA_character_) {
  value_chr <- coerce_to_character(value)
  flagged <- !is.na(value_chr) & nzchar(value_chr)

  if (!any(flagged)) {
    return(review_tbl)
  }

  bind_rows(
    review_tbl,
    tibble(
      dataset = dataset,
      model = model,
      row_id = row_ids[flagged],
      column = column,
      value = value_chr[flagged],
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

read_input_datasets <- function(cfg, project_root) {
  paths <- infer_fil2_dataset_paths(cfg, project_root)

  datasets <- list(
    gee_hb_ferritin = readr::read_csv(paths$gee_hb_ferritin, show_col_types = FALSE),
    gee_side_effect = readr::read_csv(paths$gee_side_effect, show_col_types = FALSE)
  )

  list(datasets = datasets, paths = paths)
}

coerce_numeric_with_review <- function(df, dataset_name, col, review_tbl) {
  if (!(col %in% names(df))) {
    return(list(data = df, review_tbl = review_tbl))
  }

  old <- df[[col]]
  old_chr <- coerce_to_character(old)
  parsed <- suppressWarnings(as.numeric(old_chr))

  bad_parse <- !is.na(old_chr) & nzchar(old_chr) & is.na(parsed)
  review_tbl <- append_review_flags(
    review_tbl = review_tbl,
    dataset = dataset_name,
    row_ids = df$.model_row_id[bad_parse],
    column = col,
    value = old_chr[bad_parse],
    issue = "numeric_parse_failed",
    detail = "Configured as numeric in fil3.dataset_prep but could not be parsed"
  )

  df[[col]] <- parsed
  list(data = df, review_tbl = review_tbl)
}

coerce_factor_with_review <- function(df, dataset_name, col, levels_cfg, review_tbl) {
  if (!(col %in% names(df))) {
    return(list(data = df, review_tbl = review_tbl))
  }

  old_chr <- coerce_to_character(df[[col]])
  old_chr[old_chr == ""] <- NA_character_

  if (!is.null(levels_cfg)) {
    invalid <- !is.na(old_chr) & !(old_chr %in% levels_cfg)
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      dataset = dataset_name,
      row_ids = df$.model_row_id[invalid],
      column = col,
      value = old_chr[invalid],
      issue = "unexpected_factor_level",
      detail = paste0("Allowed levels: ", paste(levels_cfg, collapse = ", "))
    )
    df[[col]] <- factor(old_chr, levels = levels_cfg)
  } else {
    df[[col]] <- factor(old_chr)
  }

  list(data = df, review_tbl = review_tbl)
}

prepare_dataset_for_models <- function(df, dataset_name, cfg, review_tbl) {
  prep_cfg <- get_cfg(cfg, paste0("fil3.dataset_prep.", dataset_name), default = list())
  factor_cols <- unlist(prep_cfg$factor_columns %||% character())
  numeric_cols <- unlist(prep_cfg$numeric_columns %||% character())
  factor_levels_cfg <- prep_cfg$factor_levels %||% list()

  df <- df %>% mutate(.model_row_id = dplyr::row_number())

  for (col in unique(numeric_cols)) {
    res <- coerce_numeric_with_review(df, dataset_name, col, review_tbl)
    df <- res$data
    review_tbl <- res$review_tbl
  }

  for (col in unique(factor_cols)) {
    col_levels <- factor_levels_cfg[[col]] %||% NULL
    res <- coerce_factor_with_review(df, dataset_name, col, col_levels, review_tbl)
    df <- res$data
    review_tbl <- res$review_tbl
  }

  list(data = df, review_tbl = review_tbl)
}

make_family_object <- function(family_name, link = NULL) {
  family_name <- tolower(family_name)

  if (family_name == "gaussian") {
    return(stats::gaussian(link = link %||% "identity"))
  }
  if (family_name == "binomial") {
    return(stats::binomial(link = link %||% "logit"))
  }
  if (family_name == "poisson") {
    return(stats::poisson(link = link %||% "log"))
  }
  if (family_name == "gamma") {
    return(stats::Gamma(link = link %||% "inverse"))
  }

  stop(sprintf("Unsupported family: '%s'", family_name), call. = FALSE)
}

validate_model_columns <- function(df, formula_str, id_col, waves_col = NULL) {
  formula_vars <- all.vars(stats::as.formula(formula_str))
  needed <- unique(c(formula_vars, id_col, waves_col))
  missing <- setdiff(needed, names(df))

  if (length(missing) > 0) {
    stop(
      sprintf(
        "Model is missing required columns: %s",
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }
}

extract_coef_table <- function(model, model_name, family_name, link_name, exponentiate = FALSE) {
  sm <- summary(model)
  coef_mat <- as.data.frame(sm$coefficients)
  coef_mat$term <- rownames(coef_mat)
  rownames(coef_mat) <- NULL

  coef_tbl <- tibble::as_tibble(coef_mat) %>%
    rename_with(~ str_replace_all(.x, "[^A-Za-z0-9]+", "_"))

  estimate_col <- names(coef_tbl)[tolower(names(coef_tbl)) %in% c("estimate")][1]
  se_col <- names(coef_tbl)[tolower(names(coef_tbl)) %in% c("std_err", "stderr", "std_error")][1]
  wald_col <- names(coef_tbl)[tolower(names(coef_tbl)) %in% c("wald")][1]
  p_col <- names(coef_tbl)[tolower(names(coef_tbl)) %in% c("pr_w", "pr_w_", "pr_wald", "pr_gt_w", "pr_greater_w", "pr_gt_chisq", "pr_chisq")][1]

  out <- coef_tbl %>%
    transmute(
      model = model_name,
      term = .data$term,
      estimate = .data[[estimate_col]],
      std_error = if (!is.na(se_col)) .data[[se_col]] else NA_real_,
      wald = if (!is.na(wald_col)) .data[[wald_col]] else NA_real_,
      p_value = if (!is.na(p_col)) .data[[p_col]] else NA_real_,
      family = family_name,
      link = link_name
    )

  if (isTRUE(exponentiate)) {
    out <- out %>%
      mutate(
        estimate_exp = exp(estimate),
        conf_low_exp = exp(estimate - 1.96 * std_error),
        conf_high_exp = exp(estimate + 1.96 * std_error)
      )
  }

  out
}

extract_qic <- function(model) {
  qic_vals <- tryCatch(geepack::QIC(model), error = function(e) NULL)

  if (is.null(qic_vals)) {
    return(list(QIC = NA_real_, QICu = NA_real_, CIC = NA_real_, Quasi_Lik = NA_real_))
  }

  qic_num <- suppressWarnings(as.numeric(qic_vals))
  qic_names <- names(qic_vals) %||% rep(NA_character_, length(qic_num))
  names(qic_num) <- qic_names

  pull_qic <- function(target_names) {
    idx <- which(names(qic_num) %in% target_names)
    if (length(idx) == 0) return(NA_real_)
    unname(qic_num[idx[1]])
  }

  list(
    QIC = pull_qic(c("QIC")),
    QICu = pull_qic(c("QICu")),
    CIC = pull_qic(c("CIC")),
    Quasi_Lik = pull_qic(c("Quasi Lik", "QuasiLik", "Quasi_Lik"))
  )
}

extract_model_info <- function(model, model_name, dataset_name, formula_str, family_name, link_name, corstr, id_col, fit_error = NULL, n_rows = NA_integer_, n_ids = NA_integer_) {
  qic_vals <- if (is.null(fit_error)) extract_qic(model) else list(QIC = NA_real_, QICu = NA_real_, CIC = NA_real_, Quasi_Lik = NA_real_)

  n_terms_val <- if (is.null(fit_error)) {
    tryCatch(length(stats::coef(model)), error = function(e) NA_integer_)
  } else {
    NA_integer_
  }
  
  converged_val <- if (is.null(fit_error)) {
    conv <- tryCatch(model$geese$converged, error = function(e) NA)
    if (is.null(conv)) NA else as.logical(conv)
  } else {
    NA
  }
  
  tibble(
    model = model_name,
    dataset = dataset_name,
    formula = formula_str,
    family = family_name,
    link = link_name,
    corstr = corstr,
    id_column = id_col,
    n_rows = n_rows,
    n_ids = n_ids,
    n_terms = n_terms_val,
    converged = converged_val,
    qic = qic_vals$QIC,
    qicu = qic_vals$QICu,
    cic = qic_vals$CIC,
    quasi_lik = qic_vals$Quasi_Lik,
    fit_status = if (is.null(fit_error)) "ok" else "failed",
    fit_error = fit_error %||% NA_character_
  )
}

fit_one_model <- function(model_name, model_cfg, datasets, cfg, review_tbl) {
  dataset_key <- require_cfg(list(dummy = model_cfg), "dummy.dataset_key")
  formula_str <- require_cfg(list(dummy = model_cfg), "dummy.formula")
  family_name <- require_cfg(list(dummy = model_cfg), "dummy.family")
  link_name <- get_cfg(list(dummy = model_cfg), "dummy.link", default = NULL)
  corstr <- get_cfg(list(dummy = model_cfg), "dummy.corstr", default = "exchangeable")
  id_col <- get_cfg(list(dummy = model_cfg), "dummy.id_column", default = "id")
  waves_col <- get_cfg(list(dummy = model_cfg), "dummy.waves_column", default = NULL)
  std_err <- get_cfg(list(dummy = model_cfg), "dummy.std_err", default = NULL)
  exponentiate <- isTRUE(get_cfg(list(dummy = model_cfg), "dummy.exponentiate", default = FALSE))

  if (!(dataset_key %in% names(datasets))) {
    stop(sprintf("Model '%s' refers to unknown dataset_key '%s'", model_name, dataset_key), call. = FALSE)
  }

  df <- datasets[[dataset_key]]
  validate_model_columns(df, formula_str, id_col, waves_col)

  fam <- make_family_object(family_name, link_name)
  args <- list(
    formula = stats::as.formula(formula_str),
    data = df,
    id = df[[id_col]],
    corstr = corstr,
    family = fam
  )

  if (!is.null(waves_col)) {
    args$waves <- df[[waves_col]]
  }
  if (!is.null(std_err)) {
    args$std.err <- std_err
  }

  fit <- tryCatch(
    do.call(geepack::geeglm, args),
    error = function(e) e
  )

  if (inherits(fit, "error")) {
    fit_error <- conditionMessage(fit)
    review_tbl <- bind_rows(
      review_tbl,
      tibble(
        dataset = dataset_key,
        model = model_name,
        row_id = NA_integer_,
        column = NA_character_,
        value = NA_character_,
        issue = "model_fit_failed",
        detail = fit_error
      )
    )

    return(list(
      fit = NULL,
      coefficients = tibble(),
      model_info = extract_model_info(
        model = NULL,
        model_name = model_name,
        dataset_name = dataset_key,
        formula_str = formula_str,
        family_name = family_name,
        link_name = fam$link,
        corstr = corstr,
        id_col = id_col,
        fit_error = fit_error,
        n_rows = nrow(df),
        n_ids = dplyr::n_distinct(df[[id_col]], na.rm = TRUE)
      ),
      review_tbl = review_tbl,
      dataset_key = dataset_key,
      save_rds_filename = get_cfg(list(dummy = model_cfg), "dummy.save_rds_filename", default = paste0(model_name, ".rds"))
    ))
  }

  coef_tbl <- extract_coef_table(
    model = fit,
    model_name = model_name,
    family_name = family_name,
    link_name = fam$link,
    exponentiate = exponentiate
  )

  info_tbl <- extract_model_info(
    model = fit,
    model_name = model_name,
    dataset_name = dataset_key,
    formula_str = formula_str,
    family_name = family_name,
    link_name = fam$link,
    corstr = corstr,
    id_col = id_col,
    fit_error = NULL,
    n_rows = nrow(df),
    n_ids = dplyr::n_distinct(df[[id_col]], na.rm = TRUE)
  )

  list(
    fit = fit,
    coefficients = coef_tbl,
    model_info = info_tbl,
    review_tbl = review_tbl,
    dataset_key = dataset_key,
    save_rds_filename = get_cfg(list(dummy = model_cfg), "dummy.save_rds_filename", default = paste0(model_name, ".rds"))
  )
}

fit_all_models <- function(datasets, cfg, review_tbl) {
  model_cfg <- require_cfg(cfg, "fil3.models")
  model_names <- names(model_cfg)

  fits <- list()
  coefficient_tables <- list()
  model_info_tables <- list()

  for (model_name in model_names) {
    this_cfg <- model_cfg[[model_name]]
    if (!isTRUE(this_cfg$enabled %||% TRUE)) {
      next
    }

    res <- fit_one_model(model_name, this_cfg, datasets, cfg, review_tbl)
    review_tbl <- res$review_tbl

    fits[[model_name]] <- res$fit
    coefficient_tables[[model_name]] <- res$coefficients
    model_info_tables[[model_name]] <- res$model_info
    if (!is.null(fits[[model_name]])) {
      attr(fits[[model_name]], "save_rds_filename") <- res$save_rds_filename
      attr(fits[[model_name]], "dataset_key") <- res$dataset_key
    }
  }

  list(
    fits = fits,
    coefficients = bind_rows(coefficient_tables),
    model_info = bind_rows(model_info_tables),
    review_tbl = review_tbl
  )
}

build_summary <- function(input_paths, model_info, review_tbl) {
  tibble(
    metric = c(
      "gee_hb_ferritin_input_path",
      "gee_side_effect_input_path",
      "processed_at_utc",
      "n_models_attempted",
      "n_models_fit_ok",
      "n_models_fit_failed",
      "n_review_flags"
    ),
    value = c(
      input_paths$gee_hb_ferritin,
      input_paths$gee_side_effect,
      format(Sys.time(), tz = "UTC", usetz = TRUE),
      nrow(model_info),
      sum(model_info$fit_status == "ok", na.rm = TRUE),
      sum(model_info$fit_status == "failed", na.rm = TRUE),
      nrow(review_tbl)
    )
  )
}

write_outputs <- function(fits, coefficients, model_info, review_tbl, summary_tbl, cfg, project_root) {
  output_dir <- file.path(project_root, get_cfg(cfg, "fil3.output.dir", default = "data/interim/fil3"))
  model_rds_dir <- file.path(
    output_dir,
    get_cfg(cfg, "fil3.output.model_rds_dir", default = "models")
  )

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(model_rds_dir, recursive = TRUE, showWarnings = FALSE)

  coefficients_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil3.output.coefficients_filename", default = "gee_coefficients.csv")
  )
  model_info_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil3.output.model_info_filename", default = "gee_model_info.csv")
  )
  review_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil3.output.review_filename", default = "fil3_review_flags.csv")
  )
  summary_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil3.output.summary_filename", default = "fil3_summary.csv")
  )
  combined_models_rds_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil3.output.combined_models_rds_filename", default = "gee_models.rds")
  )

  readr::write_csv(coefficients, coefficients_path, na = "")
  readr::write_csv(model_info, model_info_path, na = "")
  readr::write_csv(summary_tbl, summary_path, na = "")

  if (nrow(review_tbl) > 0 || isTRUE(get_cfg(cfg, "fil3.output.write_review_even_if_empty", default = TRUE))) {
    readr::write_csv(review_tbl, review_path, na = "")
  }

  saveRDS(fits, combined_models_rds_path)

  individual_paths <- list()
  for (nm in names(fits)) {
    fit_obj <- fits[[nm]]
    if (is.null(fit_obj)) {
      next
    }

    file_name <- attr(fit_obj, "save_rds_filename") %||% paste0(nm, ".rds")
    model_path <- file.path(model_rds_dir, file_name)
    saveRDS(fit_obj, model_path)
    individual_paths[[nm]] <- model_path
  }

  list(
    coefficients_path = coefficients_path,
    model_info_path = model_info_path,
    review_path = review_path,
    summary_path = summary_path,
    combined_models_rds_path = combined_models_rds_path,
    model_rds_paths = individual_paths
  )
}

run_fil3 <- function(config_path) {
  config_path <- normalizePath(config_path, mustWork = TRUE)
  project_root <- dirname(config_path)
  cfg <- yaml::read_yaml(config_path)
  review_tbl <- init_review_tbl()

  input_res <- read_input_datasets(cfg, project_root)
  datasets <- input_res$datasets
  input_paths <- input_res$paths

  prepared_datasets <- list()
  for (dataset_name in names(datasets)) {
    res <- prepare_dataset_for_models(datasets[[dataset_name]], dataset_name, cfg, review_tbl)
    prepared_datasets[[dataset_name]] <- res$data
    review_tbl <- res$review_tbl
  }

  fit_res <- fit_all_models(prepared_datasets, cfg, review_tbl)
  fits <- fit_res$fits
  coefficients <- fit_res$coefficients
  model_info <- fit_res$model_info
  review_tbl <- fit_res$review_tbl

  summary_tbl <- build_summary(input_paths, model_info, review_tbl)
  output_paths <- write_outputs(fits, coefficients, model_info, review_tbl, summary_tbl, cfg, project_root)

  list(
    models = fits,
    coefficients = coefficients,
    model_info = model_info,
    review = review_tbl,
    summary = summary_tbl,
    paths = output_paths,
    inputs = input_paths
  )
}

if (identical(environment(), globalenv()) && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript FIL3_fit_gee_models.R path/to/config.yaml", call. = FALSE)
  }

  out <- run_fil3(args[[1]])
  message("FIL 3 complete.")
  message("Coefficients CSV: ", out$paths$coefficients_path)
  message("Model info CSV:   ", out$paths$model_info_path)
  message("Review file:      ", out$paths$review_path)
  message("Summary file:     ", out$paths$summary_path)
  message("Combined models:  ", out$paths$combined_models_rds_path)
}
