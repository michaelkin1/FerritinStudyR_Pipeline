
#' FIL 5 - Final tables, figures, and report template
#'
#' Purpose:
#'   - Read prepared datasets and postprocessed model outputs from FIL 2-4
#'   - Build descriptive summary tables
#'   - Build reader-friendly model and contrast tables
#'   - Export final figures from estimated marginal means and raw data
#'   - Generate a report-ready R Markdown template
#'
#' Design principles:
#'   - No new cleaning or model fitting happens here
#'   - Everything important is config-driven
#'   - Fail-soft behaviour: ambiguous pieces are flagged in review rather than guessed
#'   - Outputs are meant to be report-ready, portfolio-ready, and easy to inspect
#'
#' Usage from another script:
#'   source("FIL5_final_outputs_report.R")
#'   out <- run_fil5("config_with_fil5.yaml")
#'
#' Usage from terminal:
#'   Rscript FIL5_final_outputs_report.R path/to/config.yaml

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(purrr)
  library(tidyr)
  library(yaml)
  library(ggplot2)
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

normalize_name <- function(x) {
  x <- tolower(x)
  gsub("[^a-z0-9]+", "", x)
}

find_matching_column <- function(df, candidates) {
  if (length(candidates) == 0 || ncol(df) == 0) return(NA_character_)
  nms <- names(df)
  nms_norm <- normalize_name(nms)
  cand_norm <- normalize_name(candidates)
  idx <- match(cand_norm, nms_norm)
  idx <- idx[!is.na(idx)]
  if (length(idx) == 0) return(NA_character_)
  nms[idx[1]]
}

ensure_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
  path
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

infer_fil5_input_paths <- function(cfg, project_root) {
  p <- function(...) file.path(project_root, ...)
  fil2_dir <- get_cfg(cfg, "fil2.output.dir", default = "data/interim/fil2")
  fil3_dir <- get_cfg(cfg, "fil3.output.dir", default = "data/interim/fil3")
  fil4_dir <- get_cfg(cfg, "fil4.output.dir", default = "data/interim/fil4")
  list(
    analysis_long = p(get_cfg(
      cfg, "fil5.input.analysis_long_path",
      default = file.path(fil2_dir, get_cfg(cfg, "fil2.output.analysis_long_filename", default = "analysis_long.csv"))
    )),
    gee_hb_ferritin = p(get_cfg(
      cfg, "fil5.input.gee_hb_ferritin_path",
      default = file.path(fil2_dir, get_cfg(cfg, "fil2.output.gee_hb_ferritin_filename", default = "gee_hb_ferritin.csv"))
    )),
    gee_side_effect = p(get_cfg(
      cfg, "fil5.input.gee_side_effect_path",
      default = file.path(fil2_dir, get_cfg(cfg, "fil2.output.gee_side_effect_filename", default = "gee_side_effect.csv"))
    )),
    coefficients = p(get_cfg(
      cfg, "fil5.input.coefficients_path",
      default = file.path(fil3_dir, get_cfg(cfg, "fil3.output.coefficients_filename", default = "gee_coefficients.csv"))
    )),
    model_info = p(get_cfg(
      cfg, "fil5.input.model_info_path",
      default = file.path(fil3_dir, get_cfg(cfg, "fil3.output.model_info_filename", default = "gee_model_info.csv"))
    )),
    emmeans = p(get_cfg(
      cfg, "fil5.input.emmeans_path",
      default = file.path(fil4_dir, get_cfg(cfg, "fil4.output.emmeans_filename", default = "emmeans_results.csv"))
    )),
    contrasts = p(get_cfg(
      cfg, "fil5.input.contrasts_path",
      default = file.path(fil4_dir, get_cfg(cfg, "fil4.output.contrasts_filename", default = "emmeans_contrasts.csv"))
    ))
  )
}

read_csv_safe <- function(path, object_name, review_tbl) {
  if (!file.exists(path)) {
    review_tbl <- append_review_row(
      review_tbl,
      stage = "read_input",
      object_name = object_name,
      issue = "input_file_missing",
      detail = paste0("File does not exist: ", path)
    )
    return(list(data = tibble(), review_tbl = review_tbl))
  }

  out <- tryCatch(
    readr::read_csv(path, show_col_types = FALSE),
    error = function(e) e
  )

  if (inherits(out, "error")) {
    review_tbl <- append_review_row(
      review_tbl,
      stage = "read_input",
      object_name = object_name,
      issue = "input_read_failed",
      detail = paste0(path, " | ", conditionMessage(out))
    )
    return(list(data = tibble(), review_tbl = review_tbl))
  }

  list(data = out, review_tbl = review_tbl)
}

read_inputs <- function(cfg, review_tbl, project_root) {
  paths <- infer_fil5_input_paths(cfg, project_root)

  analysis_long_res <- read_csv_safe(paths$analysis_long, "analysis_long", review_tbl)
  review_tbl <- analysis_long_res$review_tbl

  gee_hb_res <- read_csv_safe(paths$gee_hb_ferritin, "gee_hb_ferritin", review_tbl)
  review_tbl <- gee_hb_res$review_tbl

  gee_side_res <- read_csv_safe(paths$gee_side_effect, "gee_side_effect", review_tbl)
  review_tbl <- gee_side_res$review_tbl

  coef_res <- read_csv_safe(paths$coefficients, "coefficients", review_tbl)
  review_tbl <- coef_res$review_tbl

  info_res <- read_csv_safe(paths$model_info, "model_info", review_tbl)
  review_tbl <- info_res$review_tbl

  emm_res <- read_csv_safe(paths$emmeans, "emmeans", review_tbl)
  review_tbl <- emm_res$review_tbl

  con_res <- read_csv_safe(paths$contrasts, "contrasts", review_tbl)
  review_tbl <- con_res$review_tbl

  datasets <- list(
    analysis_long = analysis_long_res$data,
    gee_hb_ferritin = gee_hb_res$data,
    gee_side_effect = gee_side_res$data,
    coefficients = coef_res$data,
    model_info = info_res$data,
    emmeans = emm_res$data,
    contrasts = con_res$data
  )

  list(datasets = datasets, input_paths = paths, review_tbl = review_tbl)
}

coerce_numeric_if_present <- function(df, cols) {
  existing <- intersect(cols, names(df))
  for (col in existing) {
    df[[col]] <- suppressWarnings(as.numeric(coerce_to_character(df[[col]])))
  }
  df
}

prepare_inputs <- function(datasets) {
  if ("analysis_long" %in% names(datasets)) {
    datasets$analysis_long <- datasets$analysis_long |>
      coerce_numeric_if_present(c("visit_num", "hb", "ferritin", "crp", "age", "side_effect", "rls", "low_ferritin"))
  }

  if ("gee_hb_ferritin" %in% names(datasets)) {
    datasets$gee_hb_ferritin <- datasets$gee_hb_ferritin |>
      coerce_numeric_if_present(c("visit_num", "hb", "ferritin", "age"))
  }

  if ("gee_side_effect" %in% names(datasets)) {
    datasets$gee_side_effect <- datasets$gee_side_effect |>
      coerce_numeric_if_present(c("visit_num", "side_effect", "age"))
  }

  if ("coefficients" %in% names(datasets)) {
    datasets$coefficients <- datasets$coefficients |>
      coerce_numeric_if_present(c("estimate", "std_error", "wald", "p_value", "estimate_exp", "conf_low_exp", "conf_high_exp"))
  }

  datasets
}

format_p_value <- function(x) {
  out <- rep(NA_character_, length(x))
  ok <- !is.na(x)

  out[ok & x < 0.001] <- "<0.001"
  out[ok & x >= 0.001] <- sprintf("%.3f", x[ok & x >= 0.001])

  out
}

significance_stars <- function(p) {
  dplyr::case_when(
    is.na(p) ~ "",
    p < 0.001 ~ "***",
    p < 0.01 ~ "**",
    p < 0.05 ~ "*",
    TRUE ~ ""
  )
}

prettify_term <- function(term, replacements = NULL) {
  out <- term
  if (!is.null(replacements) && length(replacements) > 0) {
    for (pat in names(replacements)) {
      out <- stringr::str_replace_all(out, fixed(pat), replacements[[pat]])
    }
  }

  out |>
    stringr::str_replace_all("_", " ") |>
    stringr::str_squish()
}

summarise_numeric_by_groups <- function(df, group_vars, numeric_vars) {
  if (nrow(df) == 0 || length(group_vars) == 0 || length(numeric_vars) == 0) {
    return(tibble())
  }

  keep_group_vars <- intersect(group_vars, names(df))
  keep_numeric_vars <- intersect(numeric_vars, names(df))

  if (length(keep_group_vars) == 0 || length(keep_numeric_vars) == 0) {
    return(tibble())
  }

  df |>
    group_by(across(all_of(keep_group_vars))) |>
    summarise(
      across(
        all_of(keep_numeric_vars),
        list(
          n = ~ sum(!is.na(.x)),
          mean = ~ mean(.x, na.rm = TRUE),
          sd = ~ sd(.x, na.rm = TRUE),
          median = ~ median(.x, na.rm = TRUE),
          q1 = ~ stats::quantile(.x, probs = 0.25, na.rm = TRUE, names = FALSE),
          q3 = ~ stats::quantile(.x, probs = 0.75, na.rm = TRUE, names = FALSE),
          min = ~ suppressWarnings(min(.x, na.rm = TRUE)),
          max = ~ suppressWarnings(max(.x, na.rm = TRUE))
        ),
        .names = "{.col}__{.fn}"
      ),
      n_rows = dplyr::n(),
      n_ids = dplyr::n_distinct(.data$id),
      .groups = "drop"
    ) |>
    pivot_longer(
      cols = matches("__"),
      names_to = c("variable", ".value"),
      names_pattern = "^(.*)__(.*)$"
    ) |>
    arrange(across(all_of(keep_group_vars)), variable)
}

summarise_binary_by_groups <- function(df, group_vars, binary_vars) {
  if (nrow(df) == 0 || length(group_vars) == 0 || length(binary_vars) == 0) {
    return(tibble())
  }

  keep_group_vars <- intersect(group_vars, names(df))
  keep_binary_vars <- intersect(binary_vars, names(df))

  if (length(keep_group_vars) == 0 || length(keep_binary_vars) == 0) {
    return(tibble())
  }

  df |>
    group_by(across(all_of(keep_group_vars))) |>
    summarise(
      across(
        all_of(keep_binary_vars),
        list(
          n_non_missing = ~ sum(!is.na(.x)),
          n_positive = ~ sum(.x == 1, na.rm = TRUE),
          proportion = ~ mean(.x == 1, na.rm = TRUE)
        ),
        .names = "{.col}__{.fn}"
      ),
      n_rows = dplyr::n(),
      n_ids = dplyr::n_distinct(.data$id),
      .groups = "drop"
    ) |>
    pivot_longer(
      cols = matches("__"),
      names_to = c("variable", ".value"),
      names_pattern = "^(.*)__(.*)$"
    ) |>
    arrange(across(all_of(keep_group_vars)), variable)
}

build_descriptive_tables <- function(datasets, cfg, review_tbl) {
  tables_cfg <- get_cfg(cfg, "fil5.tables", default = list())
  out <- list()

  # Hb/Ferritin descriptive table
  if (isTRUE(get_cfg(cfg, "fil5.tables.descriptive_hb_ferritin.enabled", default = TRUE))) {
    dataset_key <- get_cfg(cfg, "fil5.tables.descriptive_hb_ferritin.dataset_key", default = "gee_hb_ferritin")
    df <- datasets[[dataset_key]] %||% tibble()

    if (nrow(df) == 0) {
      review_tbl <- append_review_row(
        review_tbl, "table", "descriptive_hb_ferritin",
        issue = "source_dataset_empty",
        detail = paste0("Dataset key: ", dataset_key)
      )
      out$descriptive_hb_ferritin <- tibble()
    } else {
      group_vars <- unlist(get_cfg(cfg, "fil5.tables.descriptive_hb_ferritin.group_by", default = c("visit_num", "sex", "group")))
      numeric_vars <- unlist(get_cfg(cfg, "fil5.tables.descriptive_hb_ferritin.numeric_vars", default = c("hb", "ferritin")))
      out$descriptive_hb_ferritin <- summarise_numeric_by_groups(df, group_vars, numeric_vars)
    }
  }

  # Side-effect descriptive table
  if (isTRUE(get_cfg(cfg, "fil5.tables.descriptive_side_effect.enabled", default = TRUE))) {
    dataset_key <- get_cfg(cfg, "fil5.tables.descriptive_side_effect.dataset_key", default = "gee_side_effect")
    df <- datasets[[dataset_key]] %||% tibble()

    if (nrow(df) == 0) {
      review_tbl <- append_review_row(
        review_tbl, "table", "descriptive_side_effect",
        issue = "source_dataset_empty",
        detail = paste0("Dataset key: ", dataset_key)
      )
      out$descriptive_side_effect <- tibble()
    } else {
      group_vars <- unlist(get_cfg(cfg, "fil5.tables.descriptive_side_effect.group_by", default = c("visit_group", "sex", "group")))
      binary_vars <- unlist(get_cfg(cfg, "fil5.tables.descriptive_side_effect.proportion_vars", default = c("side_effect")))
      out$descriptive_side_effect <- summarise_binary_by_groups(df, group_vars, binary_vars)
    }
  }

  list(tables = out, review_tbl = review_tbl)
}

build_model_results_table <- function(coefficients, model_info, cfg, review_tbl) {
  if (!isTRUE(get_cfg(cfg, "fil5.tables.model_results.enabled", default = TRUE))) {
    return(list(table = tibble(), review_tbl = review_tbl))
  }

  if (nrow(coefficients) == 0) {
    review_tbl <- append_review_row(review_tbl, "table", "model_results", issue = "coefficients_empty", detail = "No coefficient rows available")
    return(list(table = tibble(), review_tbl = review_tbl))
  }

  drop_intercept <- isTRUE(get_cfg(cfg, "fil5.tables.model_results.drop_intercept", default = TRUE))
  replacements <- get_cfg(cfg, "fil5.tables.model_results.label_replacements", default = list())

  tbl <- coefficients |>
    mutate(
      conf_low = estimate - 1.96 * std_error,
      conf_high = estimate + 1.96 * std_error
    )

  if (drop_intercept) {
    tbl <- tbl |> filter(term != "(Intercept)")
  }

  if (nrow(model_info) > 0 && "model" %in% names(model_info)) {
    info_keep <- intersect(c("model", "dataset", "formula", "family", "link", "corstr", "fit_status", "n_rows", "n_ids", "QIC", "QICu", "CIC"), names(model_info))
    tbl <- tbl |> left_join(model_info[, info_keep, drop = FALSE], by = "model", suffix = c("", "_modelinfo"))
  }

  has_exp <- all(c("estimate_exp", "conf_low_exp", "conf_high_exp") %in% names(tbl))

  tbl <- tbl |>
    mutate(
      term_label = prettify_term(term, replacements),
      estimate_display = dplyr::case_when(
        has_exp & !is.na(estimate_exp) ~ sprintf("%.2f (%.2f, %.2f)", estimate_exp, conf_low_exp, conf_high_exp),
        TRUE ~ sprintf("%.2f (%.2f, %.2f)", estimate, conf_low, conf_high)
      ),
      p_value_fmt = format_p_value(p_value),
      sig = significance_stars(p_value)
    ) |>
    arrange(model, p_value, term)

  list(table = tbl, review_tbl = review_tbl)
}

build_contrast_results_table <- function(contrasts, cfg, review_tbl) {
  if (!isTRUE(get_cfg(cfg, "fil5.tables.contrasts.enabled", default = TRUE))) {
    return(list(table = tibble(), review_tbl = review_tbl))
  }

  if (nrow(contrasts) == 0) {
    review_tbl <- append_review_row(review_tbl, "table", "contrasts", issue = "contrasts_empty", detail = "No contrast rows available")
    return(list(table = tibble(), review_tbl = review_tbl))
  }

  keep_names <- unlist(get_cfg(cfg, "fil5.tables.contrasts.keep_contrast_names", default = character()))
  tbl <- contrasts

  if (length(keep_names) > 0 && "contrast_name" %in% names(tbl)) {
    tbl <- tbl |> filter(contrast_name %in% keep_names)
  }

  estimate_col <- find_matching_column(tbl, c("estimate", "odds.ratio", "ratio", "prob", "response"))
  lower_col <- find_matching_column(tbl, c("lower.CL", "asymp.LCL", "lowerCL", "lower_cl", "lower"))
  upper_col <- find_matching_column(tbl, c("upper.CL", "asymp.UCL", "upperCL", "upper_cl", "upper"))
  p_col <- find_matching_column(tbl, c("p.value", "p_value", "p"))

  if (!is.na(estimate_col) && !is.na(lower_col) && !is.na(upper_col)) {
    tbl <- tbl |>
      mutate(
        estimate_display = sprintf(
          "%.2f (%.2f, %.2f)",
          .data[[estimate_col]],
          .data[[lower_col]],
          .data[[upper_col]]
        )
      )
  } else {
    review_tbl <- append_review_row(
      review_tbl, "table", "contrasts",
      issue = "contrast_ci_columns_not_found",
      detail = "Could not find estimate and/or CI columns for display formatting"
    )
  }

  if (!is.na(p_col)) {
    tbl <- tbl |>
      mutate(
        p_value_fmt = format_p_value(.data[[p_col]]),
        sig = significance_stars(.data[[p_col]])
      )
  }

  list(table = tbl, review_tbl = review_tbl)
}

escape_md_cell <- function(x) {
  x <- coerce_to_character(x)
  x[is.na(x)] <- ""
  x <- stringr::str_replace_all(x, "\\|", "\\\\|")
  x
}

table_to_markdown <- function(df) {
  if (nrow(df) == 0 || ncol(df) == 0) return("*(No rows to display)*\n")

  x <- df |> mutate(across(everything(), escape_md_cell))
  header <- paste0("| ", paste(names(x), collapse = " | "), " |")
  sep <- paste0("| ", paste(rep("---", ncol(x)), collapse = " | "), " |")
  rows <- apply(x, 1, function(row) paste0("| ", paste(row, collapse = " | "), " |"))
  paste(c(header, sep, rows), collapse = "\n")
}

write_table_outputs <- function(table_name, df, cfg, base_dirs) {
  if (nrow(df) == 0) return(list(csv = NA_character_, md = NA_character_))

  csv_name <- get_cfg(cfg, paste0("fil5.tables.", table_name, ".filename"), default = paste0(table_name, ".csv"))
  md_name <- get_cfg(cfg, paste0("fil5.tables.", table_name, ".markdown_filename"), default = paste0(table_name, ".md"))

  csv_path <- file.path(base_dirs$tables_dir, csv_name)
  md_path <- file.path(base_dirs$tables_dir, md_name)

  readr::write_csv(df, csv_path, na = "")
  writeLines(table_to_markdown(df), con = md_path, useBytes = TRUE)

  list(csv = csv_path, md = md_path)
}

make_emmeans_plot <- function(df, plot_name, pcfg, review_tbl) {
  if (nrow(df) == 0) {
    review_tbl <- append_review_row(review_tbl, "plot", plot_name, issue = "plot_source_empty", detail = "No rows available after filtering")
    return(list(plot = NULL, review_tbl = review_tbl))
  }

  model_name <- get_cfg(list(dummy = pcfg), "dummy.model", default = NULL)
  if (!is.null(model_name) && "model" %in% names(df)) {
    df <- df |> filter(model == model_name)
  }

  if (nrow(df) == 0) {
    review_tbl <- append_review_row(review_tbl, "plot", plot_name, issue = "plot_filter_empty", detail = paste0("No rows left after model filter: ", model_name))
    return(list(plot = NULL, review_tbl = review_tbl))
  }

  x_col <- require_cfg(list(dummy = pcfg), "dummy.x")
  color_col <- get_cfg(list(dummy = pcfg), "dummy.color", default = "group")
  facet_col <- get_cfg(list(dummy = pcfg), "dummy.facet", default = "sex")
  title <- get_cfg(list(dummy = pcfg), "dummy.title", default = plot_name)
  x_lab <- get_cfg(list(dummy = pcfg), "dummy.x_label", default = x_col)
  y_lab <- get_cfg(list(dummy = pcfg), "dummy.y_label", default = "Estimated value")

  estimate_col <- find_matching_column(df, c("emmean", "prob", "response", "rate"))
  lower_col <- find_matching_column(df, c("lower.CL", "asymp.LCL", "lowerCL", "lower_cl"))
  upper_col <- find_matching_column(df, c("upper.CL", "asymp.UCL", "upperCL", "upper_cl"))

  missing_cols <- setdiff(c(x_col, color_col, facet_col), names(df))
  if (!is.na(estimate_col)) {
    missing_cols <- setdiff(missing_cols, character())
  } else {
    missing_cols <- unique(c(missing_cols, "estimate_col"))
  }

  if (length(missing_cols) > 0) {
    review_tbl <- append_review_row(
      review_tbl, "plot", plot_name,
      issue = "plot_required_columns_missing",
      detail = paste("Missing:", paste(missing_cols, collapse = ", "))
    )
    return(list(plot = NULL, review_tbl = review_tbl))
  }

  p <- ggplot(df, aes(x = .data[[x_col]], y = .data[[estimate_col]], color = .data[[color_col]], group = .data[[color_col]])) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2)

  if (!is.na(lower_col) && !is.na(upper_col)) {
    p <- p + geom_errorbar(aes(ymin = .data[[lower_col]], ymax = .data[[upper_col]]), width = 0.12)
  }

  if (facet_col %in% names(df)) {
    p <- p + facet_wrap(stats::as.formula(paste("~", facet_col)))
  }

  if (is.character(df[[x_col]]) || is.factor(df[[x_col]])) {
    p <- p + scale_x_discrete(drop = FALSE)
  }

  p <- p +
    labs(title = title, x = x_lab, y = y_lab, color = color_col) +
    theme_minimal(base_size = 12) +
    theme(
      panel.grid.minor = element_blank(),
      legend.position = "bottom"
    )

  list(plot = p, review_tbl = review_tbl)
}

make_raw_loess_plot <- function(df, plot_name, pcfg, review_tbl) {
  if (nrow(df) == 0) {
    review_tbl <- append_review_row(review_tbl, "plot", plot_name, issue = "plot_source_empty", detail = "No rows available")
    return(list(plot = NULL, review_tbl = review_tbl))
  }

  outcome <- require_cfg(list(dummy = pcfg), "dummy.outcome")
  x_col <- get_cfg(list(dummy = pcfg), "dummy.x", default = "visit_num")
  color_col <- get_cfg(list(dummy = pcfg), "dummy.color", default = "group")
  facet_col <- get_cfg(list(dummy = pcfg), "dummy.facet", default = "sex")
  title <- get_cfg(list(dummy = pcfg), "dummy.title", default = plot_name)
  x_lab <- get_cfg(list(dummy = pcfg), "dummy.x_label", default = x_col)
  y_lab <- get_cfg(list(dummy = pcfg), "dummy.y_label", default = outcome)

  needed <- c(outcome, x_col, color_col, facet_col)
  missing <- setdiff(needed, names(df))
  if (length(missing) > 0) {
    review_tbl <- append_review_row(
      review_tbl, "plot", plot_name,
      issue = "plot_required_columns_missing",
      detail = paste("Missing:", paste(missing, collapse = ", "))
    )
    return(list(plot = NULL, review_tbl = review_tbl))
  }

  plot_df <- df |>
    filter(!is.na(.data[[outcome]]), !is.na(.data[[x_col]]), !is.na(.data[[color_col]]), !is.na(.data[[facet_col]]))

  if (nrow(plot_df) == 0) {
    review_tbl <- append_review_row(review_tbl, "plot", plot_name, issue = "plot_filter_empty", detail = "No non-missing rows after plot filtering")
    return(list(plot = NULL, review_tbl = review_tbl))
  }

  p <- ggplot(plot_df, aes(x = .data[[x_col]], y = .data[[outcome]], color = .data[[color_col]])) +
    geom_point(position = position_jitter(width = 0.08, height = 0), alpha = 0.7, size = 1.8) +
    geom_smooth(method = "loess", se = FALSE, linewidth = 1) +
    facet_wrap(stats::as.formula(paste("~", facet_col))) +
    scale_x_continuous(breaks = sort(unique(plot_df[[x_col]]))) +
    labs(title = title, x = x_lab, y = y_lab, color = color_col) +
    theme_minimal(base_size = 12) +
    theme(
      panel.grid.minor = element_blank(),
      legend.position = "bottom"
    )

  list(plot = p, review_tbl = review_tbl)
}

build_plots <- function(datasets, cfg, review_tbl) {
  plot_cfg <- get_cfg(cfg, "fil5.plots", default = list())
  out <- list()

  for (plot_name in names(plot_cfg)) {
    pcfg <- plot_cfg[[plot_name]]
    if (!isTRUE(pcfg$enabled %||% FALSE)) next

    source_table <- require_cfg(list(dummy = pcfg), "dummy.source_table")
    source_df <- datasets[[source_table]] %||% tibble()

    if (source_table == "emmeans") {
      res <- make_emmeans_plot(source_df, plot_name, pcfg, review_tbl)
    } else {
      res <- make_raw_loess_plot(source_df, plot_name, pcfg, review_tbl)
    }

    out[[plot_name]] <- res$plot
    review_tbl <- res$review_tbl
  }

  list(plots = out, review_tbl = review_tbl)
}

save_plots <- function(plots, cfg, base_dirs) {
  out <- list()

  for (plot_name in names(plots)) {
    p <- plots[[plot_name]]
    if (is.null(p)) next

    pcfg <- get_cfg(cfg, paste0("fil5.plots.", plot_name), default = list())
    filename <- get_cfg(list(dummy = pcfg), "dummy.filename", default = paste0(plot_name, ".png"))
    width <- as.numeric(get_cfg(list(dummy = pcfg), "dummy.width", default = 9))
    height <- as.numeric(get_cfg(list(dummy = pcfg), "dummy.height", default = 5))
    dpi <- as.numeric(get_cfg(list(dummy = pcfg), "dummy.dpi", default = 300))

    path <- file.path(base_dirs$figures_dir, filename)
    ggplot2::ggsave(filename = path, plot = p, width = width, height = height, dpi = dpi, units = "in")
    out[[plot_name]] <- path
  }

  out
}

build_report_rmd <- function(cfg, base_dirs, table_paths, plot_paths) {
  report_cfg <- get_cfg(cfg, "fil5.report", default = list())
  if (!isTRUE(report_cfg$enabled %||% TRUE)) return(NA_character_)

  title <- get_cfg(cfg, "fil5.report.title", default = "Järnstudie – Final summary")
  filename <- get_cfg(cfg, "fil5.report.filename", default = "final_report_template.Rmd")

  section_descriptive <- isTRUE(get_cfg(cfg, "fil5.report.include_sections.descriptive_tables", default = TRUE))
  section_model <- isTRUE(get_cfg(cfg, "fil5.report.include_sections.model_results", default = TRUE))
  section_contrasts <- isTRUE(get_cfg(cfg, "fil5.report.include_sections.contrasts", default = TRUE))
  section_figures <- isTRUE(get_cfg(cfg, "fil5.report.include_sections.figures", default = TRUE))

  to_report_relative <- function(path) {
    if (is.na(path) || is.null(path) || !nzchar(path)) return(NA_character_)
    file.path("..", basename(dirname(path)), basename(path)) |>
      stringr::str_replace_all("\\\\", "/")
  }

  lines <- c(
    "---",
    paste0('title: "', title, '"'),
    'output: html_document',
    "---",
    "",
    "```{r setup, include=FALSE}",
    "knitr::opts_chunk$set(echo = FALSE, message = FALSE, warning = FALSE)",
    "library(readr)",
    "library(dplyr)",
    "```",
    "",
    "# Overview",
    "",
    "This report template was generated by FIL 5.",
    "It is intended as a final summary layer on top of FIL 2-4 outputs.",
    ""
  )

  if (section_descriptive) {
    desc1 <- table_paths$descriptive_hb_ferritin$csv %||% NA_character_
    desc2 <- table_paths$descriptive_side_effect$csv %||% NA_character_

    if (!is.na(desc1)) {
      lines <- c(
        lines,
        "# Descriptive table – Hb and Ferritin",
        "",
        paste0("```{r}\nread_csv('", to_report_relative(desc1), "', show_col_types = FALSE)\n```"),
        ""
      )
    }

    if (!is.na(desc2)) {
      lines <- c(
        lines,
        "# Descriptive table – Side effects",
        "",
        paste0("```{r}\nread_csv('", to_report_relative(desc2), "', show_col_types = FALSE)\n```"),
        ""
      )
    }
  }

  if (section_model) {
    model_tbl <- table_paths$model_results$csv %||% NA_character_
    if (!is.na(model_tbl)) {
      lines <- c(
        lines,
        "# Model results",
        "",
        paste0("```{r}\nread_csv('", to_report_relative(model_tbl), "', show_col_types = FALSE)\n```"),
        ""
      )
    }
  }

  if (section_contrasts) {
    con_tbl <- table_paths$contrasts$csv %||% NA_character_
    if (!is.na(con_tbl)) {
      lines <- c(
        lines,
        "# Marginal means contrasts",
        "",
        paste0("```{r}\nread_csv('", to_report_relative(con_tbl), "', show_col_types = FALSE)\n```"),
        ""
      )
    }
  }

  if (section_figures && length(plot_paths) > 0) {
    lines <- c(lines, "# Figures", "")
    for (plot_name in names(plot_paths)) {
      path <- plot_paths[[plot_name]]
      if (is.na(path) || !nzchar(path)) next
      lines <- c(
        lines,
        paste0("## ", plot_name),
        "",
        paste0("![](", to_report_relative(path), ")"),
        ""
      )
    }
  }

  report_path <- file.path(base_dirs$report_dir, filename)
  writeLines(lines, con = report_path, useBytes = TRUE)

  render_flag <- isTRUE(get_cfg(cfg, "fil5.report.render", default = FALSE))
  if (render_flag && requireNamespace("rmarkdown", quietly = TRUE)) {
    rendered_name <- get_cfg(cfg, "fil5.report.rendered_filename", default = "final_report.html")
    rmarkdown::render(
      input = report_path,
      output_file = rendered_name,
      output_dir = base_dirs$report_dir,
      quiet = TRUE
    )
  }

  report_path
}

build_summary <- function(input_paths, table_paths, plot_paths, report_path, review_tbl) {
  tibble(
    metric = c(
      "n_input_files",
      "n_existing_input_files",
      "n_table_csvs_written",
      "n_markdown_tables_written",
      "n_plots_written",
      "report_template_written",
      "n_review_flags"
    ),
    value = c(
      length(input_paths),
      sum(file.exists(unlist(input_paths))),
      sum(vapply(table_paths, function(x) !is.null(x$csv) && !is.na(x$csv), logical(1))),
      sum(vapply(table_paths, function(x) !is.null(x$md) && !is.na(x$md), logical(1))),
      length(plot_paths),
      ifelse(!is.na(report_path) && file.exists(report_path), 1L, 0L),
      nrow(review_tbl)
    )
  )
}

write_outputs <- function(tables, model_results, contrast_results, plots, report_path, review_tbl, cfg, project_root) {
  output_dir <- ensure_dir(file.path(project_root, get_cfg(cfg, "fil5.output.dir", default = "outputs/fil5")))
  tables_dir <- ensure_dir(file.path(output_dir, get_cfg(cfg, "fil5.output.tables_dir", default = "tables")))
  figures_dir <- ensure_dir(file.path(output_dir, get_cfg(cfg, "fil5.output.figures_dir", default = "figures")))
  report_dir <- ensure_dir(file.path(output_dir, get_cfg(cfg, "fil5.output.report_dir", default = "report")))

  base_dirs <- list(output_dir = output_dir, tables_dir = tables_dir, figures_dir = figures_dir, report_dir = report_dir)

  table_paths <- list()
  if (!is.null(tables$descriptive_hb_ferritin)) {
    table_paths$descriptive_hb_ferritin <- write_table_outputs("descriptive_hb_ferritin", tables$descriptive_hb_ferritin, cfg, base_dirs)
  }
  if (!is.null(tables$descriptive_side_effect)) {
    table_paths$descriptive_side_effect <- write_table_outputs("descriptive_side_effect", tables$descriptive_side_effect, cfg, base_dirs)
  }
  table_paths$model_results <- write_table_outputs("model_results", model_results, cfg, base_dirs)
  table_paths$contrasts <- write_table_outputs("contrasts", contrast_results, cfg, base_dirs)

  plot_paths <- save_plots(plots, cfg, base_dirs)
  report_written_path <- build_report_rmd(cfg, base_dirs, table_paths, plot_paths)

  summary_path <- file.path(output_dir, get_cfg(cfg, "fil5.output.summary_filename", default = "fil5_summary.csv"))
  review_path <- file.path(output_dir, get_cfg(cfg, "fil5.output.review_filename", default = "fil5_review_flags.csv"))

  if (nrow(review_tbl) > 0 || isTRUE(get_cfg(cfg, "fil5.output.write_review_even_if_empty", default = TRUE))) {
    readr::write_csv(review_tbl, review_path, na = "")
  }

  list(
    base_dirs = base_dirs,
    table_paths = table_paths,
    plot_paths = plot_paths,
    report_path = report_written_path,
    summary_path = summary_path,
    review_path = review_path
  )
}

run_fil5 <- function(config_path) {
  config_path <- normalizePath(config_path, mustWork = TRUE)
  project_root <- dirname(config_path)
  cfg <- yaml::read_yaml(config_path)

  review_tbl <- init_review_tbl()

  read_res <- read_inputs(cfg, review_tbl, project_root)
  review_tbl <- read_res$review_tbl
  datasets <- prepare_inputs(read_res$datasets)

  desc_res <- build_descriptive_tables(datasets, cfg, review_tbl)
  review_tbl <- desc_res$review_tbl

  model_tbl_res <- build_model_results_table(datasets$coefficients, datasets$model_info, cfg, review_tbl)
  review_tbl <- model_tbl_res$review_tbl

  contrast_tbl_res <- build_contrast_results_table(datasets$contrasts, cfg, review_tbl)
  review_tbl <- contrast_tbl_res$review_tbl

  plot_res <- build_plots(datasets, cfg, review_tbl)
  review_tbl <- plot_res$review_tbl

  output_paths <- write_outputs(
    tables = desc_res$tables,
    model_results = model_tbl_res$table,
    contrast_results = contrast_tbl_res$table,
    plots = plot_res$plots,
    report_path = NA_character_,
    review_tbl = review_tbl,
    cfg = cfg,
    project_root = project_root
  )

  summary_tbl <- build_summary(
    read_res$input_paths,
    output_paths$table_paths,
    output_paths$plot_paths,
    output_paths$report_path,
    review_tbl
  )

  readr::write_csv(summary_tbl, output_paths$summary_path, na = "")

  list(
    paths = output_paths,
    descriptive_tables = desc_res$tables,
    model_results = model_tbl_res$table,
    contrast_results = contrast_tbl_res$table,
    review = review_tbl,
    summary = summary_tbl
  )
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  config_path <- if (length(args) >= 1) args[[1]] else "config_with_fil5.yaml"

  out <- run_fil5(config_path)

  message("FIL 5 finished.")
  message("Output dir:       ", out$paths$base_dirs$output_dir)
  message("Tables dir:       ", out$paths$base_dirs$tables_dir)
  message("Figures dir:      ", out$paths$base_dirs$figures_dir)
  message("Report dir:       ", out$paths$base_dirs$report_dir)
  message("Summary file:     ", out$paths$summary_path)
  message("Review file:      ", out$paths$review_path)
  message("Report template:  ", out$paths$report_path)
}
