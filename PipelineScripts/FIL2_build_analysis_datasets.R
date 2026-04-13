#' FIL 2 - Build analysis datasets from the clean FIL 1 CSV
#'
#' Purpose:
#'   - Read the deterministic FIL 1 clean CSV
#'   - Derive analysis variables used across models
#'   - Create model-specific datasets without fitting any models yet
#'   - Export traceable CSV outputs plus review and summary files
#'
#' Design principles:
#'   - No raw-data cleaning happens here; that belongs in FIL 1
#'   - No models are fit here; that belongs in FIL 3
#'   - Study-specific choices should live in config.yaml under `fil2`
#'   - Ambiguous cases are flagged for review rather than silently guessed
#'
#' Expected config additions under `fil2` (see config_with_fil2.yaml example):
#'   fil2:
#'     input:
#'       clean_path: "outputs/fil1/clean_long_data.csv"  # optional; can be inferred from FIL 1 output settings
#'     output:
#'       dir: "outputs/fil2"
#'       analysis_long_filename: "analysis_long.csv"
#'       gee_hb_ferritin_filename: "gee_hb_ferritin.csv"
#'       gee_side_effect_filename: "gee_side_effect.csv"
#'       review_filename: "fil2_review_flags.csv"
#'       summary_filename: "fil2_summary.csv"
#'       write_review_even_if_empty: true
#'     columns:
#'       id: "id"
#'       sex_raw: "kon"
#'       age_raw: "alder"
#'       prior_substance_raw: "tidigare_jarnsub"
#'       group_duroferon: "duroferon"
#'       group_sideral: "sideral"
#'       visit_num: "visit_num"
#'       visit_type: "visit_type"
#'       visit_code: "visit_code"
#'       hb: "hb"
#'       ferritin: "ferritin"
#'       crp: "crp"
#'       crp_operator: "crp_operator"
#'       date: "date"
#'       rls: "rls"
#'       side_effect: "side_effect"
#'     derive:
#'       group:
#'         output_column: "group"
#'         duroferon_label: "Duroferon"
#'         sideral_label: "Sideral"
#'         unknown_label: "Unknown"
#'         review_if_both_present: true
#'       sex:
#'         output_column: "sex"
#'         allowed_values: ["man", "kvinna"]
#'       age:
#'         output_column: "age"
#'       prior_substance:
#'         output_column: "prior_substance"
#'       ferritin_thresholds:
#'         low_ferritin: 30
#'         output_column: "low_ferritin"
#'     datasets:
#'       hb_ferritin:
#'         enabled: true
#'         keep_visits: [1, 2, 3, 4, 5]
#'         keep_groups: ["Duroferon", "Sideral"]
#'         require_non_missing: ["id", "sex", "age", "prior_substance", "group", "visit_num", "hb", "ferritin"]
#'         visit_factor_levels: ["1", "2", "3", "4", "5"]
#'         rls_measured_visits: [2, 3, 4]
#'         rls_factor_levels: ["0", "1"]
#'       side_effect:
#'         enabled: true
#'         keep_visits: [0, 2, 3, 4, 6]
#'         keep_groups: ["Duroferon", "Sideral"]
#'         require_non_missing: ["id", "sex", "age", "prior_substance", "group", "visit_num", "rls", "side_effect"]
#'         exclude_prior_substance: ["Inga"]
#'         visit_group_map:
#'           Baseline: [0]
#'           Mid: [2, 3]
#'           Late: [4, 6]
#'         visit_group_levels: ["Baseline", "Mid", "Late"]
#'
#' Usage from another script:
#'   source("R/FIL2_build_analysis_datasets.R")
#'   out <- run_fil2("config.yaml")
#'
#' Usage from terminal:
#'   Rscript FIL2_build_analysis_datasets.R path/to/config.yaml

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(yaml)
})

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Read a nested value from config using dot-notation.
#'
#' @param cfg Nested list-like config object.
#' @param key Dot-separated key path.
#' @param default Optional fallback value.
#' @return Config value or `default`.
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

#' Fail early if a required config key is missing.
#'
#' @param cfg Nested config object.
#' @param key Dot-separated key path.
#' @return The config value if present.
require_cfg <- function(cfg, key) {
  value <- get_cfg(cfg, key, default = NULL)
  if (is.null(value)) {
    stop(sprintf("Missing required config key: '%s'", key), call. = FALSE)
  }
  value
}

#' Convert any vector to character while preserving NA.
#'
#' @param x Vector.
#' @return Character vector.
coerce_to_character <- function(x) {
  out <- as.character(x)
  out[is.na(x)] <- NA_character_
  out
}

#' Create the standard FIL 2 review table.
#'
#' @return Empty tibble.
init_review_tbl <- function() {
  tibble(
    dataset = character(),
    row_id = integer(),
    column = character(),
    value = character(),
    issue = character(),
    detail = character()
  )
}

#' Append review flags for values requiring manual inspection.
#'
#' @param review_tbl Existing review tibble.
#' @param dataset Dataset/stage name.
#' @param row_ids Row identifiers.
#' @param column Column name.
#' @param value Value to record.
#' @param issue Short issue code.
#' @param detail Optional human-readable detail.
#' @return Updated review tibble.
append_review_flags <- function(review_tbl,
                                dataset,
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
      row_id = row_ids[flagged],
      column = column,
      value = value_chr[flagged],
      issue = issue,
      detail = detail
    )
  )
}

#' Infer FIL 1 clean CSV path if `fil2.input.clean_path` is not set.
#'
#' @param cfg Config object.
#' @return Path to FIL 1 clean CSV.
infer_fil1_clean_path <- function(cfg, project_root) {
  fil2_input <- get_cfg(cfg, "fil2.input.clean_path", default = NULL)
  if (!is.null(fil2_input)) {
    return(file.path(project_root, fil2_input))
  }

  fil1_dir <- file.path(project_root, require_cfg(cfg, "output.dir"))
  fil1_clean_filename <- require_cfg(cfg, "output.clean_filename")
  file.path(fil1_dir, fil1_clean_filename)
}

#' Read the FIL 1 clean CSV.
#'
#' @param cfg Config object.
#' @return Tibble with FIL 1 clean data.
read_clean_data <- function(cfg, project_root) {
  clean_path <- infer_fil1_clean_path(cfg, project_root)

  if (!file.exists(clean_path)) {
    stop(sprintf("FIL 1 clean CSV not found: '%s'", clean_path), call. = FALSE)
  }

  readr::read_csv(clean_path, show_col_types = FALSE)
}

#' Map source column names to FIL 2 canonical names.
#'
#' The returned named character vector has canonical names as names and source
#' columns as values.
#'
#' @param cfg Config object.
#' @return Named character vector.
get_column_map <- function(cfg) {
  cols_cfg <- get_cfg(cfg, "fil2.columns", default = list())

  defaults <- c(
    id = "id",
    sex_raw = "kon",
    age_raw = "alder",
    prior_substance_raw = "tidigare_jarnsub",
    group_duroferon = "duroferon",
    group_sideral = "sideral",
    visit_num = "visit_num",
    visit_type = "visit_type",
    visit_code = "visit_code",
    hb = "hb",
    ferritin = "ferritin",
    crp = "crp",
    crp_operator = "crp_operator",
    date = "date",
    rls = "rls",
    side_effect = "side_effect"
  )

  mapped <- defaults
  if (length(cols_cfg) > 0) {
    for (nm in names(cols_cfg)) {
      mapped[[nm]] <- cols_cfg[[nm]]
    }
  }

  mapped
}

#' Validate that needed FIL 1 columns exist.
#'
#' @param df Clean FIL 1 data.
#' @param column_map Named character vector from `get_column_map()`.
#' @return Invisibly TRUE. Stops on missing columns.
validate_input_columns <- function(df, column_map) {
  required_source_cols <- unique(unname(column_map))
  missing_cols <- setdiff(required_source_cols, names(df))

  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "FIL 2 is missing required columns from the FIL 1 CSV: %s",
        paste(missing_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

#' Derive treatment group from non-missing indicator columns.
#'
#' @param df Data frame.
#' @param duro_col Column indicating Duroferon assignment.
#' @param sideral_col Column indicating Sideral assignment.
#' @param duro_label Output label for Duroferon.
#' @param sideral_label Output label for Sideral.
#' @param unknown_label Output label when no assignment can be inferred.
#' @return Character vector with derived group labels.
derive_group_from_indicators <- function(df,
                                         duro_col,
                                         sideral_col,
                                         duro_label,
                                         sideral_label,
                                         unknown_label) {
  duro_present <- !is.na(df[[duro_col]]) & coerce_to_character(df[[duro_col]]) != ""
  sideral_present <- !is.na(df[[sideral_col]]) & coerce_to_character(df[[sideral_col]]) != ""

  dplyr::case_when(
    duro_present & !sideral_present ~ duro_label,
    sideral_present & !duro_present ~ sideral_label,
    !duro_present & !sideral_present ~ unknown_label,
    TRUE ~ unknown_label
  )
}

#' Build the general long analysis dataset.
#'
#' This is the shared input for all later model-specific datasets.
#'
#' @param clean_df FIL 1 clean data.
#' @param cfg Config object.
#' @param review_tbl Review tibble.
#' @return List with `data` and `review_tbl`.
build_analysis_long <- function(clean_df, cfg, review_tbl) {
  column_map <- get_column_map(cfg)
  validate_input_columns(clean_df, column_map)

  group_output_col <- get_cfg(cfg, "fil2.derive.group.output_column", default = "group")
  sex_output_col <- get_cfg(cfg, "fil2.derive.sex.output_column", default = "sex")
  age_output_col <- get_cfg(cfg, "fil2.derive.age.output_column", default = "age")
  prior_output_col <- get_cfg(cfg, "fil2.derive.prior_substance.output_column", default = "prior_substance")
  low_ferritin_col <- get_cfg(cfg, "fil2.derive.ferritin_thresholds.output_column", default = "low_ferritin")
  low_ferritin_threshold <- as.numeric(get_cfg(cfg, "fil2.derive.ferritin_thresholds.low_ferritin", default = 30))

  duro_label <- get_cfg(cfg, "fil2.derive.group.duroferon_label", default = "Duroferon")
  sideral_label <- get_cfg(cfg, "fil2.derive.group.sideral_label", default = "Sideral")
  unknown_label <- get_cfg(cfg, "fil2.derive.group.unknown_label", default = "Unknown")
  review_if_both_present <- isTRUE(get_cfg(cfg, "fil2.derive.group.review_if_both_present", default = TRUE))
  allowed_sex_values <- unlist(get_cfg(cfg, "fil2.derive.sex.allowed_values", default = c("man", "kvinna")))

  analysis_long <- clean_df %>%
    mutate(
      .row_id = dplyr::row_number(),
      !!group_output_col := derive_group_from_indicators(
        df = cur_data_all(),
        duro_col = column_map[["group_duroferon"]],
        sideral_col = column_map[["group_sideral"]],
        duro_label = duro_label,
        sideral_label = sideral_label,
        unknown_label = unknown_label
      ),
      !!sex_output_col := .data[[column_map[["sex_raw"]]]],
      !!age_output_col := suppressWarnings(as.numeric(.data[[column_map[["age_raw"]]]])),
      !!prior_output_col := .data[[column_map[["prior_substance_raw"]]]],
      !!low_ferritin_col := dplyr::if_else(
        !is.na(.data[[column_map[["ferritin"]]]]) & .data[[column_map[["ferritin"]]]] < low_ferritin_threshold,
        1,
        0,
        missing = 0
      )
    )

  # Canonical analysis columns, copied explicitly so later stages do not depend on
  # the raw original names.
  analysis_long <- analysis_long %>%
    mutate(
      id = .data[[column_map[["id"]]]],
      visit_num = suppressWarnings(as.numeric(.data[[column_map[["visit_num"]]]])),
      visit_type = .data[[column_map[["visit_type"]]]],
      visit_code = .data[[column_map[["visit_code"]]]],
      hb = suppressWarnings(as.numeric(.data[[column_map[["hb"]]]])),
      ferritin = suppressWarnings(as.numeric(.data[[column_map[["ferritin"]]]])),
      crp = suppressWarnings(as.numeric(.data[[column_map[["crp"]]]])),
      crp_operator = if (column_map[["crp_operator"]] %in% names(analysis_long)) .data[[column_map[["crp_operator"]]]] else NA_character_,
      date = .data[[column_map[["date"]]]],
      rls = suppressWarnings(as.numeric(.data[[column_map[["rls"]]]])),
      side_effect = suppressWarnings(as.numeric(.data[[column_map[["side_effect"]]]])),
      group = .data[[group_output_col]],
      sex = .data[[sex_output_col]],
      age = .data[[age_output_col]],
      prior_substance = .data[[prior_output_col]],
      low_ferritin = .data[[low_ferritin_col]]
    )

  # Review: both group indicators present.
  duro_present <- !is.na(analysis_long[[column_map[["group_duroferon"]]]]) & coerce_to_character(analysis_long[[column_map[["group_duroferon"]]]]) != ""
  sideral_present <- !is.na(analysis_long[[column_map[["group_sideral"]]]]) & coerce_to_character(analysis_long[[column_map[["group_sideral"]]]]) != ""
  both_group_present <- duro_present & sideral_present

  if (review_if_both_present) {
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      dataset = "analysis_long",
      row_ids = analysis_long$.row_id[both_group_present],
      column = "group",
      value = analysis_long$group[both_group_present],
      issue = "conflicting_group_indicators",
      detail = sprintf(
        "Both '%s' and '%s' were non-missing; group set to '%s'",
        column_map[["group_duroferon"]],
        column_map[["group_sideral"]],
        unknown_label
      )
    )
  }

  # Review: unknown group.
  unknown_group_mask <- !is.na(analysis_long$id) & analysis_long$group == unknown_label
  review_tbl <- append_review_flags(
    review_tbl = review_tbl,
    dataset = "analysis_long",
    row_ids = analysis_long$.row_id[unknown_group_mask],
    column = "group",
    value = analysis_long$group[unknown_group_mask],
    issue = "unknown_group_assignment",
    detail = "Could not infer treatment group from indicator columns"
  )

  # Review: unexpected sex values.
  bad_sex_mask <- !is.na(analysis_long$sex) & !(analysis_long$sex %in% allowed_sex_values)
  review_tbl <- append_review_flags(
    review_tbl = review_tbl,
    dataset = "analysis_long",
    row_ids = analysis_long$.row_id[bad_sex_mask],
    column = "sex",
    value = analysis_long$sex[bad_sex_mask],
    issue = "unexpected_sex_value",
    detail = paste0("Allowed values: ", paste(allowed_sex_values, collapse = ", "))
  )

  # Review: duplicate id + visit rows.
  dup_mask <- duplicated(analysis_long[c("id", "visit_num")]) | duplicated(analysis_long[c("id", "visit_num")], fromLast = TRUE)
  review_tbl <- append_review_flags(
    review_tbl = review_tbl,
    dataset = "analysis_long",
    row_ids = analysis_long$.row_id[dup_mask],
    column = "id/visit_num",
    value = paste0(analysis_long$id[dup_mask], " / ", analysis_long$visit_num[dup_mask]),
    issue = "duplicate_id_visit",
    detail = "More than one row shares the same id and visit_num"
  )

  list(data = analysis_long, review_tbl = review_tbl)
}

#' Flag rows with missing required fields for a given dataset.
#'
#' @param df Dataset.
#' @param dataset_name Dataset name.
#' @param required_cols Character vector of required columns.
#' @param review_tbl Review tibble.
#' @return Updated review tibble.
flag_missing_required_values <- function(df, dataset_name, required_cols, review_tbl) {
  required_cols <- intersect(required_cols, names(df))

  if (length(required_cols) == 0) {
    return(review_tbl)
  }

  for (col in required_cols) {
    miss <- is.na(df[[col]]) | (is.character(df[[col]]) & df[[col]] == "")
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      dataset = dataset_name,
      row_ids = df$.row_id[miss],
      column = col,
      value = df[[col]][miss],
      issue = "missing_required_value",
      detail = "Required for this analysis dataset"
    )
  }

  review_tbl
}

#' Build the Hb/Ferritin GEE dataset.
#'
#' This dataset is filtered and derived for the repeated-measures biomarker models.
#'
#' @param analysis_long Shared analysis-long dataset.
#' @param cfg Config object.
#' @param review_tbl Review tibble.
#' @return List with `data` and `review_tbl`.
build_hb_ferritin_dataset <- function(analysis_long, cfg, review_tbl) {
  enabled <- isTRUE(get_cfg(cfg, "fil2.datasets.hb_ferritin.enabled", default = TRUE))
  if (!enabled) {
    return(list(data = tibble(), review_tbl = review_tbl))
  }

  keep_visits <- as.numeric(unlist(get_cfg(cfg, "fil2.datasets.hb_ferritin.keep_visits", default = c(1, 2, 3, 4, 5))))
  keep_groups <- unlist(get_cfg(cfg, "fil2.datasets.hb_ferritin.keep_groups", default = c("Duroferon", "Sideral")))
  required_cols <- unlist(get_cfg(
    cfg,
    "fil2.datasets.hb_ferritin.require_non_missing",
    default = c("id", "sex", "age", "prior_substance", "group", "visit_num", "hb", "ferritin")
  ))
  visit_factor_levels <- as.character(unlist(get_cfg(cfg, "fil2.datasets.hb_ferritin.visit_factor_levels", default = c("1", "2", "3", "4", "5"))))
  rls_measured_visits <- as.numeric(unlist(get_cfg(cfg, "fil2.datasets.hb_ferritin.rls_measured_visits", default = c(2, 3, 4))))
  rls_factor_levels <- as.character(unlist(get_cfg(cfg, "fil2.datasets.hb_ferritin.rls_factor_levels", default = c("0", "1"))))

  candidate_df <- analysis_long %>%
    filter(visit_num %in% keep_visits)

  review_tbl <- flag_missing_required_values(candidate_df, "gee_hb_ferritin_candidate", required_cols, review_tbl)

  hb_df <- candidate_df %>%
    filter(
      group %in% keep_groups,
      !if_any(all_of(required_cols), is.na)
    ) %>%
    mutate(
      id = as.character(id),
      group = factor(group, levels = keep_groups) %>% as.character(),
      sex = as.character(sex),
      prior_substance = as.character(prior_substance),
      age = as.numeric(age),
      visit_num_factor = factor(as.character(visit_num), levels = visit_factor_levels) %>% as.character(),
      rls_model = dplyr::case_when(
        visit_num %in% rls_measured_visits ~ as.character(rls),
        TRUE ~ NA_character_
      ),
      rls_model = if_else(!is.na(rls_model) & rls_model %in% rls_factor_levels, rls_model, NA_character_)
    )

  # Review rows that survived visit filtering but failed the final dataset filters.
  excluded_idx <- !(candidate_df$.row_id %in% hb_df$.row_id)
  excluded_rows <- candidate_df[excluded_idx, , drop = FALSE]
  if (nrow(excluded_rows) > 0) {
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      dataset = "gee_hb_ferritin",
      row_ids = excluded_rows$.row_id,
      column = "dataset_filter",
      value = excluded_rows$id,
      issue = "excluded_from_hb_ferritin_dataset",
      detail = "Failed group and/or required-value filters for Hb/Ferritin dataset"
    )
  }

  list(data = hb_df, review_tbl = review_tbl)
}

#' Build the side-effect GEE dataset.
#'
#' This dataset is filtered and derived for the repeated-measures side-effect model.
#'
#' @param analysis_long Shared analysis-long dataset.
#' @param cfg Config object.
#' @param review_tbl Review tibble.
#' @return List with `data` and `review_tbl`.
build_side_effect_dataset <- function(analysis_long, cfg, review_tbl) {
  enabled <- isTRUE(get_cfg(cfg, "fil2.datasets.side_effect.enabled", default = TRUE))
  if (!enabled) {
    return(list(data = tibble(), review_tbl = review_tbl))
  }

  keep_visits <- as.numeric(unlist(get_cfg(cfg, "fil2.datasets.side_effect.keep_visits", default = c(0, 2, 3, 4, 6))))
  keep_groups <- unlist(get_cfg(cfg, "fil2.datasets.side_effect.keep_groups", default = c("Duroferon", "Sideral")))
  required_cols <- unlist(get_cfg(
    cfg,
    "fil2.datasets.side_effect.require_non_missing",
    default = c("id", "sex", "age", "prior_substance", "group", "visit_num", "rls", "side_effect")
  ))
  exclude_prior_substance <- unlist(get_cfg(cfg, "fil2.datasets.side_effect.exclude_prior_substance", default = c("Inga")))
  visit_group_map <- get_cfg(cfg, "fil2.datasets.side_effect.visit_group_map", default = list(Baseline = c(0), Mid = c(2, 3), Late = c(4, 6)))
  visit_group_levels <- unlist(get_cfg(cfg, "fil2.datasets.side_effect.visit_group_levels", default = c("Baseline", "Mid", "Late")))

  candidate_df <- analysis_long %>%
    filter(visit_num %in% keep_visits)

  review_tbl <- flag_missing_required_values(candidate_df, "gee_side_effect_candidate", required_cols, review_tbl)

  side_df <- candidate_df %>%
    filter(
      group %in% keep_groups,
      !if_any(all_of(required_cols), is.na),
      !(prior_substance %in% exclude_prior_substance)
    )

  side_df$visit_group <- NA_character_
  for (lvl in names(visit_group_map)) {
    these_visits <- as.numeric(unlist(visit_group_map[[lvl]]))
    side_df$visit_group[side_df$visit_num %in% these_visits] <- lvl
  }

  side_df <- side_df %>%
    mutate(
      id = as.character(id),
      group = factor(group, levels = keep_groups) %>% as.character(),
      sex = as.character(sex),
      prior_substance = as.character(prior_substance),
      age = as.numeric(age),
      visit_num_factor = as.character(visit_num),
      visit_group = factor(visit_group, levels = visit_group_levels) %>% as.character(),
      rls = as.numeric(rls),
      side_effect = as.numeric(side_effect)
    )

  # Review any visits that were kept but not mapped to a visit_group.
  bad_visit_group <- is.na(side_df$visit_group)
  review_tbl <- append_review_flags(
    review_tbl = review_tbl,
    dataset = "gee_side_effect",
    row_ids = side_df$.row_id[bad_visit_group],
    column = "visit_group",
    value = side_df$visit_num[bad_visit_group],
    issue = "visit_group_map_missing",
    detail = "visit_num was not covered by fil2.datasets.side_effect.visit_group_map"
  )

  excluded_idx <- !(candidate_df$.row_id %in% side_df$.row_id)
  excluded_rows <- candidate_df[excluded_idx, , drop = FALSE]
  if (nrow(excluded_rows) > 0) {
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      dataset = "gee_side_effect",
      row_ids = excluded_rows$.row_id,
      column = "dataset_filter",
      value = excluded_rows$id,
      issue = "excluded_from_side_effect_dataset",
      detail = "Failed group, prior_substance, and/or required-value filters for side-effect dataset"
    )
  }

  list(data = side_df, review_tbl = review_tbl)
}

#' Build a FIL 2 summary table.
#'
#' @param clean_df FIL 1 clean data.
#' @param analysis_long Shared analysis-long data.
#' @param hb_df Hb/Ferritin dataset.
#' @param side_df Side-effect dataset.
#' @param review_tbl FIL 2 review flags.
#' @param cfg Config object.
#' @return Tibble.
build_summary <- function(clean_df, analysis_long, hb_df, side_df, review_tbl, cfg, project_root) {
  tibble(
    metric = c(
      "fil1_clean_input_path",
      "processed_at_utc",
      "fil1_clean_n_rows",
      "analysis_long_n_rows",
      "analysis_long_n_ids",
      "gee_hb_ferritin_n_rows",
      "gee_hb_ferritin_n_ids",
      "gee_side_effect_n_rows",
      "gee_side_effect_n_ids",
      "n_review_flags"
    ),
    value = c(
      infer_fil1_clean_path(cfg, project_root),
      format(Sys.time(), tz = "UTC", usetz = TRUE),
      nrow(clean_df),
      nrow(analysis_long),
      dplyr::n_distinct(analysis_long$id, na.rm = TRUE),
      nrow(hb_df),
      dplyr::n_distinct(hb_df$id, na.rm = TRUE),
      nrow(side_df),
      dplyr::n_distinct(side_df$id, na.rm = TRUE),
      nrow(review_tbl)
    )
  )
}

#' Write FIL 2 outputs to disk.
#'
#' @param analysis_long Shared analysis-long data.
#' @param hb_df Hb/Ferritin dataset.
#' @param side_df Side-effect dataset.
#' @param review_tbl FIL 2 review flags.
#' @param summary_tbl FIL 2 summary table.
#' @param cfg Config object.
#' @return Named list of output paths.
write_outputs <- function(analysis_long, hb_df, side_df, review_tbl, summary_tbl, cfg, project_root) {
  output_dir <- file.path(project_root, get_cfg(cfg, "fil2.output.dir", default = "data/interim/fil2"))
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  analysis_long_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil2.output.analysis_long_filename", default = "analysis_long.csv")
  )
  hb_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil2.output.gee_hb_ferritin_filename", default = "gee_hb_ferritin.csv")
  )
  side_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil2.output.gee_side_effect_filename", default = "gee_side_effect.csv")
  )
  review_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil2.output.review_filename", default = "fil2_review_flags.csv")
  )
  summary_path <- file.path(
    output_dir,
    get_cfg(cfg, "fil2.output.summary_filename", default = "fil2_summary.csv")
  )

  readr::write_csv(analysis_long, analysis_long_path, na = "")
  readr::write_csv(hb_df, hb_path, na = "")
  readr::write_csv(side_df, side_path, na = "")
  readr::write_csv(summary_tbl, summary_path, na = "")

  if (nrow(review_tbl) > 0 || isTRUE(get_cfg(cfg, "fil2.output.write_review_even_if_empty", default = TRUE))) {
    readr::write_csv(review_tbl, review_path, na = "")
  }

  list(
    analysis_long_path = analysis_long_path,
    gee_hb_ferritin_path = hb_path,
    gee_side_effect_path = side_path,
    review_path = review_path,
    summary_path = summary_path
  )
}

#' Run the full FIL 2 step.
#'
#' @param config_path Path to YAML config file.
#' @return Named list containing datasets and output paths.
run_fil2 <- function(config_path) {
  config_path <- normalizePath(config_path, mustWork = TRUE)
  project_root <- dirname(config_path)
  cfg <- yaml::read_yaml(config_path)
  clean_df <- read_clean_data(cfg, project_root)
  review_tbl <- init_review_tbl()

  res <- build_analysis_long(clean_df, cfg, review_tbl)
  analysis_long <- res$data
  review_tbl <- res$review_tbl

  res <- build_hb_ferritin_dataset(analysis_long, cfg, review_tbl)
  hb_df <- res$data
  review_tbl <- res$review_tbl

  res <- build_side_effect_dataset(analysis_long, cfg, review_tbl)
  side_df <- res$data
  review_tbl <- res$review_tbl

  # Internal helper row id is useful for traceability but should not leak into the
  # exported analysis datasets unless explicitly wanted later.
  analysis_long_export <- analysis_long %>% select(-any_of(".row_id"))
  hb_export <- hb_df %>% select(-any_of(".row_id"))
  side_export <- side_df %>% select(-any_of(".row_id"))

  summary_tbl <- build_summary(clean_df, analysis_long_export, hb_export, side_export, review_tbl, cfg, project_root)
  output_paths <- write_outputs(analysis_long_export, hb_export, side_export, review_tbl, summary_tbl, cfg, project_root)

  list(
    analysis_long = analysis_long_export,
    gee_hb_ferritin = hb_export,
    gee_side_effect = side_export,
    review = review_tbl,
    summary = summary_tbl,
    paths = output_paths
  )
}

# Allow execution with: Rscript FIL2_build_analysis_datasets.R config.yaml
if (identical(environment(), globalenv()) && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop("Usage: Rscript FIL2_build_analysis_datasets.R path/to/config.yaml", call. = FALSE)
  }

  out <- run_fil2(args[[1]])
  message("FIL 2 complete.")
  message("Analysis long:    ", out$paths$analysis_long_path)
  message("Hb/Ferritin CSV: ", out$paths$gee_hb_ferritin_path)
  message("Side-effect CSV: ", out$paths$gee_side_effect_path)
  message("Review file:      ", out$paths$review_path)
  message("Summary file:     ", out$paths$summary_path)
}
