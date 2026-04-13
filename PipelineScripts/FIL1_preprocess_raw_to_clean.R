#' FIL 1 - Preprocess raw data into a clean, traceable analysis-ready CSV
#'
#' Purpose:
#'   - Read raw data without overwriting the original file
#'   - Apply deterministic, config-driven cleaning rules
#'   - Export a cleaned dataset, change log, review file, and summary
#'
#' Design principles:
#'   - No study-specific paths or values are hardcoded here
#'   - All domain rules should live in config.yaml
#'   - Safe auto-fixes are applied deterministically
#'   - Ambiguous values are flagged for manual review rather than guessed
#'
#' Expected high-level config structure:
#'   input:
#'     path: "path/to/raw.xlsx"
#'     sheet: 3
#'     file_type: "xlsx"     # optional; inferred from extension if omitted
#'
#'   output:
#'     dir: "outputs/fil1"
#'     clean_filename: "clean_data.csv"
#'     log_filename: "change_log.csv"
#'     review_filename: "review_flags.csv"
#'     summary_filename: "processing_summary.csv"
#'     copy_raw_filename: "raw_snapshot.xlsx"   # optional
#'     write_review_even_if_empty: true
#'     write_log_even_if_empty: true
#'
#'   columns:
#'     clean_names: true
#'     required: ["id"]
#'     expected: []
#'
#'   cleaning:
#'     trim_whitespace: true
#'     na_strings: ["NA", "N/A", "", " "]
#'     empty_strings_to_na: true
#'
#'   id_standardization:
#'     columns: ["id"]
#'     remove_trailing_period: true
#'     strip_all_whitespace: false
#'
#'   value_maps:
#'     tidigare_jarnsub:
#'       columns: ["tidigare_jarnsub"]
#'       ignore_case: true
#'       map:
#'         "Niferex dr.": "Niferex"
#'         "Niferexdroppar": "Niferex"
#'     group_raw:
#'       columns: ["tidigare_jarnsub"]
#'       ignore_case: true
#'       map:
#'         "Duroferon": "Duroferon"
#'         "Niferex/Duroferon": "Niferex/Duroferon"
#'         "Inga": "Inga"
#'
#'   yes_no_maps:
#'     biv:
#'       columns: ["biv"]
#'       map:
#'         "J": 1
#'         "N": 0
#'       invalid_to_na: ["?", "J?", "N?"]
#'       output_suffix: "_std"
#'
#'   numeric_columns:
#'     - column: "crp"
#'       decimal_comma: true
#'       allow_operators: ["<", ">", "<=", ">="]
#'       extract_numeric_part: true
#'       parsed_column: "crp_num"
#'       operator_column: "crp_operator"
#'     - column: "hb"
#'       decimal_comma: true
#'       parsed_column: "hb"
#'
#'   date_columns:
#'     - column: "blodgivning"
#'       orders: ["ymd", "Ymd", "dmy", "dmY", "ymd HMS"]
#'       parsed_column: "blodgivning_date"
#'
#'   validation:
#'     allowed_values:
#'       tidigare_jarnsub: ["Niferex", "Duroferon", "Niferex/Duroferon", "Inga"]
#'     review_unknown_values: true
#'     stop_on_missing_required_columns: true
#'
#'   reshape:
#'     enabled: true
#'     cols_regex: "_(\\d+|k1|05|ff)$"
#'     names_to: ["variable", "visit_code"]
#'     names_pattern: "^(.*)_(\\d+|k1|05|ff)$"
#'     values_to: "value"
#'     values_drop_na: false
#'     id_columns: ["id"]
#'     visit_code_map:
#'       "k1": 99
#'       "05": 5
#'       "ff": 6
#'     visit_type_map:
#'       "k1": "control"
#'       "05": "followup"
#'       "ff": "followup_form"
#'     parsed_visit_num_column: "visit_num"
#'     parsed_visit_type_column: "visit_type"
#'
#' Usage from another script:
#'   source("PipelineScripts/FIL1_preprocess_raw_to_clean.R")
#'   out <- run_fil1("config.yaml")
#'
#' Usage from terminal:
#'   Rscript PipelineScripts/FIL1_preprocess_raw_to_clean.R path/to/config.yaml

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(janitor)
  library(yaml)
  library(tibble)
  library(lubridate)
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

#' Append change-log rows for values that changed.
#'
#' @param log_tbl Existing log tibble.
#' @param row_ids Row identifiers.
#' @param column Column name.
#' @param old Old values.
#' @param new New values.
#' @param rule Rule identifier.
#' @param note Optional note.
#' @return Updated log tibble.
append_change_log <- function(log_tbl,
                              row_ids,
                              column,
                              old,
                              new,
                              rule,
                              note = NA_character_) {
  old_chr <- coerce_to_character(old)
  new_chr <- coerce_to_character(new)
  
  changed <- !(is.na(old_chr) & is.na(new_chr)) &
    ((is.na(old_chr) != is.na(new_chr)) |
       (!is.na(old_chr) & !is.na(new_chr) & old_chr != new_chr))
  
  if (!any(changed)) {
    return(log_tbl)
  }
  
  bind_rows(
    log_tbl,
    tibble(
      row_id = row_ids[changed],
      column = column,
      old_value = old_chr[changed],
      new_value = new_chr[changed],
      rule = rule,
      note = note
    )
  )
}

#' Append review flags for values requiring manual inspection.
#'
#' @param review_tbl Existing review tibble.
#' @param row_ids Row identifiers.
#' @param column Column name.
#' @param value Original/current value.
#' @param issue Short issue code.
#' @param detail Optional human-readable detail.
#' @return Updated review tibble.
append_review_flags <- function(review_tbl,
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
      row_id = row_ids[flagged],
      column = column,
      value = value_chr[flagged],
      issue = issue,
      detail = detail
    )
  )
}

#' Apply a vectorized transformation to selected columns and log exact changes.
#'
#' @param df Data frame.
#' @param cols Columns to transform.
#' @param rule Rule identifier.
#' @param fn Function taking a vector and returning a vector of same length.
#' @param log_tbl Existing log tibble.
#' @return List with updated `df` and `log_tbl`.
apply_transform_with_log <- function(df, cols, rule, fn, log_tbl) {
  cols <- intersect(cols, names(df))
  
  if (length(cols) == 0) {
    return(list(df = df, log_tbl = log_tbl))
  }
  
  for (col in cols) {
    old <- df[[col]]
    new <- fn(old)
    df[[col]] <- new
    log_tbl <- append_change_log(
      log_tbl = log_tbl,
      # BUG FIX 2: .row_id may not exist on the data frame if it was not yet
      # added (e.g. when this helper is called before mutate(.row_id)).
      # Guard with a fallback to seq_len so the function never silently uses
      # a NULL index, which would produce zero-row log entries and lose all
      # traceability.
      row_ids = if (".row_id" %in% names(df)) df$.row_id else seq_len(nrow(df)),
      column = col,
      old = old,
      new = new,
      rule = rule
    )
  }
  
  list(df = df, log_tbl = log_tbl)
}

#' Safely detect spreadsheet or text input type.
#'
#' @param path Input file path.
#' @param declared_type Optional type from config.
#' @return Lowercase file type.
resolve_file_type <- function(path, declared_type = NULL) {
  if (!is.null(declared_type)) {
    return(tolower(declared_type))
  }
  tolower(tools::file_ext(path))
}

#' Read raw input file without modifying it.
#'
#' @param cfg Config object.
#' @return Raw data frame.
read_raw_data <- function(cfg, project_root) {
  input_path <- file.path(project_root, require_cfg(cfg, "input.path"))
  file_type <- resolve_file_type(input_path, get_cfg(cfg, "input.file_type"))
  na_values <- get_cfg(cfg, "cleaning.na_strings", default = c("NA", "N/A", ""))
  
  if (!file.exists(input_path)) {
    stop(sprintf("Input file does not exist: %s", input_path), call. = FALSE)
  }
  
  if (file_type %in% c("xlsx", "xls")) {
    sheet <- get_cfg(cfg, "input.sheet", default = 1)
    df <- readxl::read_excel(
      path = input_path,
      sheet = sheet,
      na = na_values,
      .name_repair = "minimal"
    )
  } else if (file_type %in% c("csv", "txt", "tsv")) {
    delim <- get_cfg(cfg, "input.delim", default = ifelse(file_type == "tsv", "\t", ","))
    df <- readr::read_delim(
      file = input_path,
      delim = delim,
      na = na_values,
      show_col_types = FALSE,
      progress = FALSE,
      trim_ws = FALSE
    )
  } else {
    stop(sprintf("Unsupported input file type: %s", file_type), call. = FALSE)
  }
  
  as_tibble(df)
}

# BUG FIX 1: The original function contained a verbatim copy of itself as a
# nested inner function, but never called or returned it.  The outer shell
# therefore always returned NULL invisibly, which cascaded and made every
# downstream res$df assignment NULL.  The inner duplicate has been removed;
# the correct logic now lives directly in the outer function body.
#
#' Optionally standardize column names with janitor::clean_names().
#'
#' Any column-name changes are also written to the change log using row_id = NA.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param log_tbl Existing log tibble.
#' @return List with updated `df` and `log_tbl`.
clean_column_names_if_needed <- function(df, cfg, log_tbl) {
  do_clean <- isTRUE(get_cfg(cfg, "columns.clean_names", default = TRUE))
  if (!do_clean) {
    return(list(df = df, log_tbl = log_tbl))
  }
  
  old_names <- names(df)
  names(df) <- janitor::make_clean_names(names(df))
  new_names <- names(df)
  
  changed <- old_names != new_names
  if (any(changed)) {
    log_tbl <- bind_rows(
      log_tbl,
      tibble(
        row_id = NA_integer_,
        column = old_names[changed],
        old_value = old_names[changed],
        new_value = new_names[changed],
        rule = "clean_column_names",
        note = NA_character_
      )
    )
  }
  
  list(df = df, log_tbl = log_tbl)
}

#' Validate required and expected columns.
#'
#' Missing required columns can either stop the run or be written to review.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param review_tbl Existing review tibble.
#' @return List with updated `review_tbl`.
validate_columns <- function(df, cfg, review_tbl) {
  required_cols <- get_cfg(cfg, "columns.required", default = character())
  expected_cols <- get_cfg(cfg, "columns.expected", default = character())
  stop_on_missing_required <- isTRUE(
    get_cfg(cfg, "validation.stop_on_missing_required_columns", default = TRUE)
  )
  
  missing_required <- setdiff(required_cols, names(df))
  missing_expected <- setdiff(expected_cols, names(df))
  
  if (length(missing_required) > 0) {
    if (stop_on_missing_required) {
      stop(
        sprintf(
          "Missing required columns: %s",
          paste(missing_required, collapse = ", ")
        ),
        call. = FALSE
      )
    }
    
    review_tbl <- bind_rows(
      review_tbl,
      tibble(
        row_id = NA_integer_,
        column = missing_required,
        value = NA_character_,
        issue = "missing_required_column",
        detail = "Required column missing from input"
      )
    )
  }
  
  if (length(missing_expected) > 0) {
    review_tbl <- bind_rows(
      review_tbl,
      tibble(
        row_id = NA_integer_,
        column = missing_expected,
        value = NA_character_,
        issue = "missing_expected_column",
        detail = "Expected column missing from input"
      )
    )
  }
  
  unexpected_cols <- setdiff(names(df), unique(c(required_cols, expected_cols)))
  report_unexpected <- isTRUE(
    get_cfg(cfg, "validation.report_unexpected_columns", default = FALSE)
  )
  if (report_unexpected && length(expected_cols) > 0 && length(unexpected_cols) > 0) {
    review_tbl <- bind_rows(
      review_tbl,
      tibble(
        row_id = NA_integer_,
        column = unexpected_cols,
        value = NA_character_,
        issue = "unexpected_column",
        detail = "Column not listed under columns.expected"
      )
    )
  }
  
  list(review_tbl = review_tbl)
}

#' Standardize ID-like fields.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param log_tbl Existing change log.
#' @return List with updated `df` and `log_tbl`.
standardize_id_columns <- function(df, cfg, log_tbl) {
  cols <- get_cfg(cfg, "id_standardization.columns", default = character())
  cols <- intersect(cols, names(df))
  
  if (length(cols) == 0) {
    return(list(df = df, log_tbl = log_tbl))
  }
  
  remove_trailing_period <- isTRUE(
    get_cfg(cfg, "id_standardization.remove_trailing_period", default = TRUE)
  )
  strip_all_whitespace <- isTRUE(
    get_cfg(cfg, "id_standardization.strip_all_whitespace", default = FALSE)
  )
  
  id_fn <- function(x) {
    x_chr <- coerce_to_character(x)
    x_chr <- ifelse(is.na(x_chr), NA_character_, stringr::str_trim(x_chr))
    
    if (remove_trailing_period) {
      x_chr <- stringr::str_replace(x_chr, "\\.$", "")
    }
    
    if (strip_all_whitespace) {
      x_chr <- stringr::str_replace_all(x_chr, "\\s+", "")
    }
    
    x_chr
  }
  
  apply_transform_with_log(df, cols, "standardize_id", id_fn, log_tbl)
}

#' Apply config-driven synonym maps to categorical values.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param log_tbl Existing change log.
#' @return List with updated `df` and `log_tbl`.
apply_value_maps <- function(df, cfg, log_tbl) {
  value_maps <- get_cfg(cfg, "value_maps", default = list())
  
  if (length(value_maps) == 0) {
    return(list(df = df, log_tbl = log_tbl))
  }
  
  for (map_name in names(value_maps)) {
    rule_cfg <- value_maps[[map_name]]
    cols <- intersect(rule_cfg$columns %||% character(), names(df))
    mapping <- rule_cfg$map %||% list()
    ignore_case <- isTRUE(rule_cfg$ignore_case %||% TRUE)
    
    if (length(cols) == 0 || length(mapping) == 0) {
      next
    }
    
    if (ignore_case) {
      key_lookup <- setNames(unname(unlist(mapping)), tolower(names(mapping)))
      map_fn <- function(x) {
        x_chr <- coerce_to_character(x)
        idx <- match(tolower(x_chr), names(key_lookup))
        mapped <- unname(key_lookup[idx])
        ifelse(!is.na(idx), mapped, x_chr)
      }
    } else {
      key_lookup <- setNames(unname(unlist(mapping)), names(mapping))
      map_fn <- function(x) {
        x_chr <- coerce_to_character(x)
        idx <- match(x_chr, names(key_lookup))
        mapped <- unname(key_lookup[idx])
        ifelse(!is.na(idx), mapped, x_chr)
      }
    }
    
    res <- apply_transform_with_log(df, cols, paste0("value_map:", map_name), map_fn, log_tbl)
    df <- res$df
    log_tbl <- res$log_tbl
  }
  
  list(df = df, log_tbl = log_tbl)
}

#' Standardize yes/no fields into explicit coded columns.
#'
#' This does not overwrite the original source column unless parsed_column is
#' set equal to the original column in config.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param log_tbl Existing change log.
#' @param review_tbl Existing review flags.
#' @return List with updated `df`, `log_tbl`, and `review_tbl`.
apply_yes_no_maps <- function(df, cfg, log_tbl, review_tbl) {
  yes_no_maps <- get_cfg(cfg, "yes_no_maps", default = list())
  
  if (length(yes_no_maps) == 0) {
    return(list(df = df, log_tbl = log_tbl, review_tbl = review_tbl))
  }
  
  for (rule_name in names(yes_no_maps)) {
    rule_cfg <- yes_no_maps[[rule_name]]
    cols <- intersect(rule_cfg$columns %||% character(), names(df))
    mapping <- rule_cfg$map %||% list()
    invalid_to_na <- toupper(unlist(rule_cfg$invalid_to_na %||% character()))
    output_suffix <- rule_cfg$output_suffix %||% "_std"
    
    if (length(cols) == 0 || length(mapping) == 0) {
      next
    }
    
    mapping_chr <- unlist(mapping)
    map_lookup <- setNames(mapping_chr, toupper(names(mapping_chr)))
    
    for (col in cols) {
      src <- coerce_to_character(df[[col]])
      src_trim <- ifelse(is.na(src), NA_character_, stringr::str_trim(src))
      src_upper <- toupper(src_trim)
      
      parsed <- src_upper
      parsed[src_upper %in% invalid_to_na] <- NA_character_
      idx <- match(parsed, names(map_lookup))
      new_values <- map_lookup[idx]
      new_values[is.na(idx)] <- NA_character_
      new_values[is.na(src_trim)] <- NA_character_
      
      parsed_col <- rule_cfg$parsed_column %||% paste0(col, output_suffix)
      old_target <- if (parsed_col %in% names(df)) df[[parsed_col]] else rep(NA_character_, nrow(df))
      df[[parsed_col]] <- type.convert(new_values, as.is = TRUE)
      
      log_tbl <- append_change_log(
        log_tbl = log_tbl,
        row_ids = df$.row_id,
        column = parsed_col,
        old = old_target,
        new = df[[parsed_col]],
        rule = paste0("yes_no_map:", rule_name),
        note = paste0("Derived from source column '", col, "'")
      )
      
      invalid_mask <- !is.na(src_trim) & !(src_upper %in% c(names(map_lookup), invalid_to_na))
      review_tbl <- append_review_flags(
        review_tbl = review_tbl,
        row_ids = df$.row_id[invalid_mask],
        column = col,
        value = src_trim[invalid_mask],
        issue = "invalid_yes_no_value",
        detail = paste0("No mapping found for rule '", rule_name, "'")
      )
    }
  }
  
  list(df = df, log_tbl = log_tbl, review_tbl = review_tbl)
}

#' Standardize decimal commas and parse numeric fields.
#'
#' Operator-bearing values such as <5 can be split into operator and numeric
#' part when configured. These values are flagged for manual review by default.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param log_tbl Existing change log.
#' @param review_tbl Existing review flags.
#' @return List with updated `df`, `log_tbl`, and `review_tbl`.
apply_numeric_parsing <- function(df, cfg, log_tbl, review_tbl) {
  numeric_rules <- get_cfg(cfg, "numeric_columns", default = list())
  
  if (length(numeric_rules) == 0) {
    return(list(df = df, log_tbl = log_tbl, review_tbl = review_tbl))
  }
  
  for (rule_cfg in numeric_rules) {
    col <- rule_cfg$column %||% NULL
    if (is.null(col) || !(col %in% names(df))) {
      next
    }
    
    decimal_comma <- isTRUE(rule_cfg$decimal_comma %||% TRUE)
    parsed_col <- rule_cfg$parsed_column %||% col
    operator_col <- rule_cfg$operator_column %||% paste0(col, "_operator")
    allow_ops <- rule_cfg$allow_operators %||% character()
    extract_numeric <- isTRUE(rule_cfg$extract_numeric_part %||% FALSE)
    flag_operator_values <- isTRUE(rule_cfg$flag_operator_values %||% TRUE)
    
    old_target <- if (parsed_col %in% names(df)) df[[parsed_col]] else rep(NA_real_, nrow(df))
    src <- coerce_to_character(df[[col]])
    src_trim <- ifelse(is.na(src), NA_character_, stringr::str_trim(src))
    
    normalized <- src_trim
    if (decimal_comma) {
      normalized <- stringr::str_replace_all(normalized, ",", ".")
    }
    
    operator <- rep(NA_character_, length(normalized))
    numeric_text <- normalized
    
    if (length(allow_ops) > 0) {
      ops_pattern <- paste(sort(allow_ops, decreasing = TRUE), collapse = "|")
      op_match <- stringr::str_match(normalized, paste0("^\\s*(", ops_pattern, ")\\s*(.+?)\\s*$"))
      operator <- op_match[, 2]
      numeric_text <- ifelse(!is.na(operator), op_match[, 3], normalized)
    }
    
    if (extract_numeric) {
      numeric_text <- stringr::str_extract(numeric_text, "[-+]?[0-9]*\\.?[0-9]+")
    }
    
    parsed_numeric <- suppressWarnings(as.numeric(numeric_text))
    parsed_numeric[is.na(src_trim)] <- NA_real_
    df[[parsed_col]] <- parsed_numeric
    
    log_tbl <- append_change_log(
      log_tbl = log_tbl,
      row_ids = df$.row_id,
      column = parsed_col,
      old = old_target,
      new = df[[parsed_col]],
      rule = paste0("numeric_parse:", col)
    )
    
    if (length(allow_ops) > 0) {
      old_operator <- if (operator_col %in% names(df)) df[[operator_col]] else rep(NA_character_, nrow(df))
      df[[operator_col]] <- operator
      log_tbl <- append_change_log(
        log_tbl = log_tbl,
        row_ids = df$.row_id,
        column = operator_col,
        old = old_operator,
        new = df[[operator_col]],
        rule = paste0("numeric_operator_extract:", col)
      )
      
      if (flag_operator_values) {
        op_mask <- !is.na(operator)
        review_tbl <- append_review_flags(
          review_tbl = review_tbl,
          row_ids = df$.row_id[op_mask],
          column = col,
          value = src_trim[op_mask],
          issue = "operator_numeric_value",
          detail = paste0("Operator preserved in column '", operator_col, "'")
        )
      }
    }
    
    failed_mask <- !is.na(src_trim) & is.na(parsed_numeric)
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      row_ids = df$.row_id[failed_mask],
      column = col,
      value = src_trim[failed_mask],
      issue = "numeric_parse_failed",
      detail = "Could not parse numeric value"
    )
  }
  
  list(df = df, log_tbl = log_tbl, review_tbl = review_tbl)
}

#' Parse date columns using config-defined lubridate orders.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param log_tbl Existing change log.
#' @param review_tbl Existing review flags.
#' @return List with updated `df`, `log_tbl`, and `review_tbl`.
apply_date_parsing <- function(df, cfg, log_tbl, review_tbl) {
  date_rules <- get_cfg(cfg, "date_columns", default = list())
  
  if (length(date_rules) == 0) {
    return(list(df = df, log_tbl = log_tbl, review_tbl = review_tbl))
  }
  
  for (rule_cfg in date_rules) {
    col <- rule_cfg$column %||% NULL
    if (is.null(col) || !(col %in% names(df))) {
      next
    }
    
    parsed_col <- rule_cfg$parsed_column %||% paste0(col, "_date")
    orders <- unlist(rule_cfg$orders %||% c("ymd", "Ymd", "dmy", "dmY"))
    tz <- rule_cfg$tz %||% "UTC"
    
    old_target <- if (parsed_col %in% names(df)) df[[parsed_col]] else as.Date(rep(NA, nrow(df)))
    src <- coerce_to_character(df[[col]])
    src_trim <- ifelse(is.na(src), NA_character_, stringr::str_trim(src))
    
    parsed <- suppressWarnings(
      lubridate::parse_date_time(src_trim, orders = orders, tz = tz, exact = FALSE)
    )
    parsed_date <- as.Date(parsed)
    parsed_date[is.na(src_trim)] <- as.Date(NA)
    df[[parsed_col]] <- parsed_date
    
    log_tbl <- append_change_log(
      log_tbl = log_tbl,
      row_ids = df$.row_id,
      column = parsed_col,
      old = old_target,
      new = df[[parsed_col]],
      rule = paste0("date_parse:", col)
    )
    
    failed_mask <- !is.na(src_trim) & is.na(parsed_date)
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      row_ids = df$.row_id[failed_mask],
      column = col,
      value = src_trim[failed_mask],
      issue = "date_parse_failed",
      detail = paste0("Failed to parse using orders: ", paste(orders, collapse = ", "))
    )
  }
  
  list(df = df, log_tbl = log_tbl, review_tbl = review_tbl)
}

#' Validate allowed categorical values after cleaning.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param review_tbl Existing review flags.
#' @return Updated review tibble.
validate_allowed_values <- function(df, cfg, review_tbl) {
  allowed_cfg <- get_cfg(cfg, "validation.allowed_values", default = list())
  review_unknown <- isTRUE(get_cfg(cfg, "validation.review_unknown_values", default = TRUE))
  
  if (!review_unknown || length(allowed_cfg) == 0) {
    return(review_tbl)
  }
  
  for (col in names(allowed_cfg)) {
    if (!(col %in% names(df))) {
      next
    }
    
    allowed <- unlist(allowed_cfg[[col]])
    values <- coerce_to_character(df[[col]])
    bad_mask <- !is.na(values) & !(values %in% allowed)
    
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      row_ids = df$.row_id[bad_mask],
      column = col,
      value = values[bad_mask],
      issue = "unknown_category",
      detail = paste0("Allowed values: ", paste(allowed, collapse = ", "))
    )
  }
  
  review_tbl
}

# BUG FIX 3: visit_code_map and visit_type_map in config.yaml use string keys
# ("k1", "05", "ff") but also plain numeric strings ("1", "2", ...) for
# scheduled visits.  The original lookup used list subsetting with a character
# vector index (visit_code_map[raw_visit_code]), which silently returns NULL
# for any key not present in the named list — including all the plain numeric
# visit codes.  This made visit_num NA for every scheduled visit row.
#
# Fix: use explicit match() against names(visit_code_map) so that only the
# special codes ("k1", "05", "ff") are translated and numeric strings fall
# through cleanly to as.numeric() conversion.
#
#' Reshape visit-style wide data to long format when configured.
#'
#' @param df Data frame.
#' @param cfg Config object.
#' @param review_tbl Existing review flags.
#' @return List with updated `df` and `review_tbl`.
reshape_if_needed <- function(df, cfg, review_tbl) {
  enabled <- isTRUE(get_cfg(cfg, "reshape.enabled", default = FALSE))
  if (!enabled) {
    return(list(df = df, review_tbl = review_tbl))
  }
  
  cols_regex    <- require_cfg(cfg, "reshape.cols_regex")
  names_to      <- get_cfg(cfg, "reshape.names_to", default = c("variable", "visit_code"))
  names_pattern <- require_cfg(cfg, "reshape.names_pattern")
  values_to     <- get_cfg(cfg, "reshape.values_to", default = "value")
  values_drop_na <- isTRUE(get_cfg(cfg, "reshape.values_drop_na", default = FALSE))
  visit_code_map <- get_cfg(cfg, "reshape.visit_code_map", default = list())
  visit_type_map <- get_cfg(cfg, "reshape.visit_type_map", default = list())
  visit_num_col  <- get_cfg(cfg, "reshape.parsed_visit_num_column", default = "visit_num")
  visit_type_col <- get_cfg(cfg, "reshape.parsed_visit_type_column", default = "visit_type")
  
  id_cols <- get_cfg(cfg, "reshape.id_columns", default = NULL)
  if (is.null(id_cols)) {
    id_cols <- names(df)[!stringr::str_detect(names(df), cols_regex)]
  }
  id_cols <- intersect(id_cols, names(df))
  
  long_df <- df %>%
    mutate(across(matches(cols_regex), as.character)) %>%
    pivot_longer(
      cols         = matches(cols_regex),
      names_to     = names_to,
      names_pattern = names_pattern,
      values_to    = values_to,
      values_drop_na = values_drop_na
    ) %>%
    pivot_wider(
      names_from  = all_of(names_to[[1]]),
      values_from = all_of(values_to)
    )
  
  if ("visit_code" %in% names(long_df)) {
    raw_visit_code <- coerce_to_character(long_df$visit_code)
    
    # BUG FIX 3 (continued): use match() so we never get NULL from list[].
    special_keys <- names(visit_code_map)
    special_idx  <- match(raw_visit_code, special_keys)
    mapped_visit_num <- ifelse(
      !is.na(special_idx),
      as.numeric(unlist(visit_code_map)[special_idx]),
      suppressWarnings(as.numeric(raw_visit_code))
    )
    
    special_type_keys <- names(visit_type_map)
    special_type_idx  <- match(raw_visit_code, special_type_keys)
    numeric_visit_code <- suppressWarnings(as.numeric(raw_visit_code))
    mapped_visit_type <- ifelse(
      !is.na(special_type_idx),
      unlist(visit_type_map)[special_type_idx],
      ifelse(!is.na(numeric_visit_code), "scheduled", NA_character_)
    )
    
    long_df[[visit_num_col]]  <- mapped_visit_num
    long_df[[visit_type_col]] <- mapped_visit_type
    
    bad_visit_mask <- is.na(long_df[[visit_num_col]]) & !is.na(raw_visit_code)
    review_tbl <- append_review_flags(
      review_tbl = review_tbl,
      row_ids    = long_df$.row_id[bad_visit_mask],
      column     = "visit_code",
      value      = raw_visit_code[bad_visit_mask],
      issue      = "unknown_visit_code",
      detail     = "Could not map visit code to numeric visit"
    )
  }
  
  list(df = long_df, review_tbl = review_tbl)
}

#' Build a processing summary table.
#'
#' @param raw_df Raw input data.
#' @param clean_df Cleaned output data.
#' @param log_tbl Change log.
#' @param review_tbl Review flags.
#' @param cfg Config object.
#' @return Tibble with one row per summary metric.
build_summary <- function(raw_df, clean_df, log_tbl, review_tbl, cfg) {
  input_path <- require_cfg(cfg, "input.path")
  
  tibble(
    metric = c(
      "input_path",
      "processed_at_utc",
      "raw_n_rows",
      "raw_n_cols",
      "clean_n_rows",
      "clean_n_cols",
      "n_logged_changes",
      "n_review_flags"
    ),
    value = c(
      input_path,
      format(Sys.time(), tz = "UTC", usetz = TRUE),
      nrow(raw_df),
      ncol(raw_df),
      nrow(clean_df),
      ncol(clean_df),
      nrow(log_tbl),
      nrow(review_tbl)
    )
  )
}

#' Write all FIL 1 outputs to disk.
#'
#' @param raw_df Raw data.
#' @param clean_df Cleaned data.
#' @param log_tbl Change log.
#' @param review_tbl Review flags.
#' @param summary_tbl Summary metrics.
#' @param cfg Config object.
#' @return Named list with output paths.
write_outputs <- function(raw_df, clean_df, log_tbl, review_tbl, summary_tbl, cfg, project_root) {
  output_dir <- file.path(project_root, require_cfg(cfg, "output.dir"))
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  
  clean_path   <- file.path(output_dir, get_cfg(cfg, "output.clean_filename",   default = "clean_data.csv"))
  log_path     <- file.path(output_dir, get_cfg(cfg, "output.log_filename",     default = "change_log.csv"))
  review_path  <- file.path(output_dir, get_cfg(cfg, "output.review_filename",  default = "review_flags.csv"))
  summary_path <- file.path(output_dir, get_cfg(cfg, "output.summary_filename", default = "processing_summary.csv"))
  
  readr::write_csv(clean_df,    clean_path,   na = "")
  readr::write_csv(summary_tbl, summary_path, na = "")
  
  if (nrow(log_tbl) > 0 || isTRUE(get_cfg(cfg, "output.write_log_even_if_empty", default = TRUE))) {
    readr::write_csv(log_tbl, log_path, na = "")
  }
  
  if (nrow(review_tbl) > 0 || isTRUE(get_cfg(cfg, "output.write_review_even_if_empty", default = TRUE))) {
    readr::write_csv(review_tbl, review_path, na = "")
  }
  
  copy_raw_filename <- get_cfg(cfg, "output.copy_raw_filename", default = NULL)
  raw_copy_path <- NULL
  if (!is.null(copy_raw_filename)) {
    raw_copy_path <- file.path(output_dir, copy_raw_filename)
    file.copy(from = file.path(project_root, require_cfg(cfg, "input.path")), to = raw_copy_path, overwrite = FALSE)
  }
  
  list(
    clean_path   = clean_path,
    log_path     = log_path,
    review_path  = review_path,
    summary_path = summary_path,
    raw_copy_path = raw_copy_path
  )
}

# BUG FIX 4: Path handling.  config.yaml uses a relative output.dir
# ("data/interim/fil1") which only resolves correctly when R's working
# directory is the project root.  run_fil1() now resolves the config path to
# an absolute path first, then temporarily sets the working directory to the
# directory that *contains* the config file for the duration of the run.  This
# means the script works correctly whether it is called from the project root,
# from another directory, or via Rscript on the command line, as long as
# config.yaml lives in the project root.
#
#' Run the full FIL 1 preprocessing step.
#'
#' @param config_path Path to YAML config file (absolute or relative to the
#'   current working directory at call time).
#' @return Named list containing cleaned data and output paths.
run_fil1 <- function(config_path) {
  # Resolve to absolute path before any directory changes.
  config_path <- normalizePath(config_path, mustWork = TRUE)
  project_root <- dirname(config_path)
  cfg <- yaml::read_yaml(config_path)

  raw_df <- read_raw_data(cfg, project_root)
  
  log_tbl <- tibble(
    row_id    = integer(),
    column    = character(),
    old_value = character(),
    new_value = character(),
    rule      = character(),
    note      = character()
  )
  
  review_tbl <- tibble(
    row_id = integer(),
    column = character(),
    value  = character(),
    issue  = character(),
    detail = character()
  )
  
  work_df <- raw_df %>%
    mutate(.row_id = dplyr::row_number())
  
  # Column names first, so downstream config refers to standardized names.
  res     <- clean_column_names_if_needed(work_df, cfg, log_tbl)
  work_df <- res$df
  log_tbl <- res$log_tbl
  
  res        <- validate_columns(work_df, cfg, review_tbl)
  review_tbl <- res$review_tbl
  
  # Generic character cleanup.
  char_cols <- names(work_df)[vapply(work_df, is.character, logical(1))]
  
  if (isTRUE(get_cfg(cfg, "cleaning.trim_whitespace", default = TRUE))) {
    res     <- apply_transform_with_log(
      work_df,
      cols = char_cols,
      rule = "trim_whitespace",
      fn   = function(x) ifelse(is.na(x), NA_character_, stringr::str_trim(coerce_to_character(x))),
      log_tbl = log_tbl
    )
    work_df <- res$df
    log_tbl <- res$log_tbl
  }
  
  na_strings <- get_cfg(cfg, "cleaning.na_strings", default = c("NA", "N/A", ""))
  na_strings <- unique(c(
    na_strings,
    if (isTRUE(get_cfg(cfg, "cleaning.empty_strings_to_na", default = TRUE))) "" else NULL
  ))
  
  res     <- apply_transform_with_log(
    work_df,
    cols = char_cols,
    rule = "standardize_na_strings",
    fn   = function(x) {
      x_chr <- coerce_to_character(x)
      x_chr[x_chr %in% na_strings] <- NA_character_
      x_chr
    },
    log_tbl = log_tbl
  )
  work_df <- res$df
  log_tbl <- res$log_tbl
  
  res     <- standardize_id_columns(work_df, cfg, log_tbl)
  work_df <- res$df
  log_tbl <- res$log_tbl
  
  res     <- apply_value_maps(work_df, cfg, log_tbl)
  work_df <- res$df
  log_tbl <- res$log_tbl
  
  # Reshape before typed parsing when the raw file is wide.  This ensures
  # visit-level variables like hb/ferritin/crp/blodgivning can be parsed after
  # pivoting.
  res        <- reshape_if_needed(work_df, cfg, review_tbl)
  work_df    <- res$df
  review_tbl <- res$review_tbl
  
  # Re-apply value maps after reshape as well.  This is harmless for
  # already-clean non-visit columns and necessary when reshaped variables
  # create new columns.
  res     <- apply_value_maps(work_df, cfg, log_tbl)
  work_df <- res$df
  log_tbl <- res$log_tbl
  
  res        <- apply_yes_no_maps(work_df, cfg, log_tbl, review_tbl)
  work_df    <- res$df
  log_tbl    <- res$log_tbl
  review_tbl <- res$review_tbl
  
  res        <- apply_numeric_parsing(work_df, cfg, log_tbl, review_tbl)
  work_df    <- res$df
  log_tbl    <- res$log_tbl
  review_tbl <- res$review_tbl
  
  res        <- apply_date_parsing(work_df, cfg, log_tbl, review_tbl)
  work_df    <- res$df
  log_tbl    <- res$log_tbl
  review_tbl <- res$review_tbl
  
  review_tbl <- validate_allowed_values(work_df, cfg, review_tbl)
  
  # Keep row provenance but drop internal helper column from final export
  # unless explicitly requested in config.
  keep_row_id <- isTRUE(get_cfg(cfg, "output.keep_row_id", default = FALSE))
  clean_df <- if (keep_row_id) work_df else select(work_df, -any_of(".row_id"))
  
  summary_tbl  <- build_summary(raw_df, clean_df, log_tbl, review_tbl, cfg)
  output_paths <- write_outputs(raw_df, clean_df, log_tbl, review_tbl, summary_tbl, cfg, project_root)
  
  list(
    data       = clean_df,
    change_log = log_tbl,
    review     = review_tbl,
    summary    = summary_tbl,
    paths      = output_paths
  )
}

# ---------------------------------------------------------------------------
# Command-line entry point
# Allow execution with:
#   Rscript PipelineScripts/FIL1_preprocess_raw_to_clean.R path/to/config.yaml
# ---------------------------------------------------------------------------
if (identical(environment(), globalenv()) && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    stop(
      "Usage: Rscript PipelineScripts/FIL1_preprocess_raw_to_clean.R path/to/config.yaml",
      call. = FALSE
    )
  }
  
  out <- run_fil1(args[[1]])
  message("FIL 1 complete.")
  message("Clean data:   ", out$paths$clean_path)
  message("Change log:   ", out$paths$log_path)
  message("Review file:  ", out$paths$review_path)
  message("Summary file: ", out$paths$summary_path)
}