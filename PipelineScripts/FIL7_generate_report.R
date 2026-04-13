#' FIL 7 - Generate narrative report (Word + HTML)
#'
#' Purpose:
#'   - Read FIL3-FIL5 outputs (coefficients, emmeans, contrasts, plots)
#'   - Scan for significant findings across all models
#'   - Generate a structured Methods + Results report as both .docx and .html
#'   - Embed plots with figure captions and inline statistical commentary
#'
#' Usage:
#'   source("PipelineScripts/FIL7_generate_report.R")
#'   run_fil7("config.yaml")
#'
#' Rscript PipelineScripts/FIL7_generate_report.R config.yaml

suppressPackageStartupMessages({
  library(yaml)
  library(dplyr)
  library(readr)
  library(tibble)
  library(stringr)
  library(officer)
  library(flextable)
})

# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

`%||%` <- function(x, y) if (is.null(x)) y else x

get_cfg <- function(cfg, key, default = NULL) {
  parts <- strsplit(key, "\\.")[[1]]
  value <- cfg
  for (part in parts) {
    if (!is.list(value) || is.null(value[[part]])) return(default)
    value <- value[[part]]
  }
  value
}

ensure_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

# ─────────────────────────────────────────────────────────────────────────────
# P-value formatting
# ─────────────────────────────────────────────────────────────────────────────

fmt_p <- function(p) {
  dplyr::case_when(
    is.na(p)    ~ "NA",
    p < 0.001   ~ "p < 0.001",
    p < 0.01    ~ sprintf("p = %.3f", p),
    p < 0.05    ~ sprintf("p = %.3f", p),
    TRUE        ~ sprintf("p = %.2f", p)
  )
}

sig_stars <- function(p) {
  dplyr::case_when(
    is.na(p)  ~ "",
    p < 0.001 ~ "***",
    p < 0.01  ~ "**",
    p < 0.05  ~ "*",
    TRUE      ~ ""
  )
}
fmt_estimate <- function(x, digits = 2) {
  if (is.na(x)) return("NA")
  sprintf("%s", round(x, digits))
}

fmt_or <- function(est, lo, hi) {
  if (any(is.na(c(est, lo, hi)))) return("OR = NA")
  sprintf("OR = %.2f (95%% CI: %.2f\u2013%.2f)", est, lo, hi)
}

# ─────────────────────────────────────────────────────────────────────────────
# Clean term labels
# ─────────────────────────────────────────────────────────────────────────────

clean_term <- function(term) {
  term %>%
    str_replace_all("visit_num_factor(\\d+)", "Visit \\1") %>%
    str_replace_all("visit_groupMid", "Visit group: Mid") %>%
    str_replace_all("visit_groupLate", "Visit group: Late") %>%
    str_replace_all("sexkvinna", "Sex: Female") %>%
    str_replace_all("groupSideral", "Group: Sideral") %>%
    str_replace_all("prior_substance", "Prior iron: ") %>%
    str_replace_all("age", "Age") %>%
    str_replace_all("rls1", "RLS: Yes") %>%
    str_replace_all(":", " \u00d7 ")
}

# ─────────────────────────────────────────────────────────────────────────────
# Read all inputs
# ─────────────────────────────────────────────────────────────────────────────

read_all_inputs <- function(cfg, project_root) {
  p <- function(key, default) {
    file.path(project_root, get_cfg(cfg, key, default = default))
  }

  list(
    coefficients  = readr::read_csv(p("fil5.input.coefficients_path",  "data/interim/fil3/gee_coefficients.csv"),  show_col_types = FALSE),
    model_info    = readr::read_csv(p("fil5.input.model_info_path",     "data/interim/fil3/gee_model_info.csv"),    show_col_types = FALSE),
    emmeans       = readr::read_csv(p("fil5.input.emmeans_path",        "data/interim/fil4/emmeans_results.csv"),   show_col_types = FALSE),
    contrasts     = readr::read_csv(p("fil5.input.contrasts_path",      "data/interim/fil4/emmeans_contrasts.csv"), show_col_types = FALSE),
    plots = list(
      hb_emmeans         = file.path(project_root, get_cfg(cfg, "fil5.output.dir", "outputs/fil5"), "figures", "plot_hb_emmeans.png"),
      ferritin_emmeans   = file.path(project_root, get_cfg(cfg, "fil5.output.dir", "outputs/fil5"), "figures", "plot_ferritin_emmeans.png"),
      side_effect_emmeans= file.path(project_root, get_cfg(cfg, "fil5.output.dir", "outputs/fil5"), "figures", "plot_side_effect_emmeans.png"),
      hb_raw_loess       = file.path(project_root, get_cfg(cfg, "fil5.output.dir", "outputs/fil5"), "figures", "plot_hb_raw_loess.png"),
      ferritin_raw_loess = file.path(project_root, get_cfg(cfg, "fil5.output.dir", "outputs/fil5"), "figures", "plot_ferritin_raw_loess.png")
    )
  )
}

# ─────────────────────────────────────────────────────────────────────────────
# Finding extraction
# ─────────────────────────────────────────────────────────────────────────────

extract_findings <- function(coef, alpha = 0.05) {
  coef %>%
    filter(!str_detect(term, "Intercept")) %>%
    mutate(
      significant = !is.na(p_value) & p_value < alpha,
      label = clean_term(term),
      stars = sig_stars(p_value),
      p_fmt = fmt_p(p_value)
    )
}

findings_for_model <- function(findings, model_name) {
  findings %>% filter(model == model_name)
}

# Build a short narrative paragraph for one model's key findings
narrative_gaussian <- function(findings, outcome_label, unit = "") {
  sig <- findings %>% filter(significant)
  nonsig <- findings %>% filter(!significant)

  lines <- character()

  # Significant findings
  if (nrow(sig) > 0) {
    for (i in seq_len(nrow(sig))) {
      r <- sig[i, ]
      direction <- if (r$estimate > 0) "higher" else "lower"
      lines <- c(lines, sprintf(
        "%s was associated with a statistically significant difference in %s (estimate = %s %s, SE = %s, %s%s).",
        r$label, outcome_label,
        fmt_estimate(r$estimate), unit,
        fmt_estimate(r$std_error),
        r$p_fmt, if (nzchar(r$stars)) paste0(" ", r$stars) else ""
      ))
    }
  } else {
    lines <- c(lines, sprintf(
      "No predictors reached statistical significance for %s (all p \u2265 0.05).",
      outcome_label
    ))
  }

  # Highlight a few notable non-significant findings
  notable_nonsig <- nonsig %>%
    filter(str_detect(term, "groupSideral$|sexkvinna$")) %>%
    slice_head(n = 2)

  if (nrow(notable_nonsig) > 0) {
    for (i in seq_len(nrow(notable_nonsig))) {
      r <- notable_nonsig[i, ]
      lines <- c(lines, sprintf(
        "Notably, %s did not reach statistical significance (estimate = %s %s, %s), suggesting no detectable effect on %s.",
        r$label,
        fmt_estimate(r$estimate), unit,
        r$p_fmt,
        outcome_label
      ))
    }
  }

  paste(lines, collapse = " ")
}

narrative_binomial <- function(findings, outcome_label) {
  sig <- findings %>% filter(significant)
  nonsig <- findings %>% filter(!significant)

  lines <- character()

  if (nrow(sig) > 0) {
    for (i in seq_len(nrow(sig))) {
      r <- sig[i, ]
      or_str <- if (!is.na(r$estimate_exp)) {
        fmt_or(r$estimate_exp, r$conf_low_exp, r$conf_high_exp)
      } else {
        sprintf("estimate = %.2f", r$estimate)
      }
      lines <- c(lines, sprintf(
        "%s was significantly associated with %s (%s, %s%s).",
        r$label, outcome_label,
        or_str,
        r$p_fmt,
        if (nzchar(r$stars)) paste0(" ", r$stars) else ""
      ))
    }
  } else {
    lines <- c(lines, sprintf(
      "No predictors reached statistical significance for %s (all p \u2265 0.05).",
      outcome_label
    ))
  }

  # Notable non-significant
  notable_nonsig <- nonsig %>%
    filter(str_detect(term, "groupSideral$|sexkvinna$|age$")) %>%
    slice_head(n = 2)

  if (nrow(notable_nonsig) > 0) {
    for (i in seq_len(nrow(notable_nonsig))) {
      r <- notable_nonsig[i, ]
      or_str <- if (!is.na(r$estimate_exp)) {
        fmt_or(r$estimate_exp, r$conf_low_exp, r$conf_high_exp)
      } else {
        sprintf("estimate = %.2f", r$estimate)
      }
      lines <- c(lines, sprintf(
        "%s did not reach statistical significance (%s, %s), suggesting no detectable independent effect on %s.",
        r$label, or_str, r$p_fmt, outcome_label
      ))
    }
  }

  paste(lines, collapse = " ")
}

# ─────────────────────────────────────────────────────────────────────────────
# Coefficient summary table (flextable-ready)
# ─────────────────────────────────────────────────────────────────────────────

make_coef_table <- function(coef, model_name, is_binomial = FALSE) {
  df <- coef %>%
    filter(model == model_name, !str_detect(term, "Intercept")) %>%
    mutate(
      Term = clean_term(term),
      Estimate = round(estimate, 3),
      SE = round(std_error, 3),
      Wald = round(wald, 3),
      `p-value` = case_when(
        p_value < 0.001 ~ "< 0.001 ***",
        p_value < 0.01  ~ sprintf("%.3f **", p_value),
        p_value < 0.05  ~ sprintf("%.3f *", p_value),
        TRUE            ~ sprintf("%.3f", p_value)
      )
    )

  if (is_binomial) {
    df <- df %>%
      mutate(
        OR = round(estimate_exp, 3),
        `95% CI` = sprintf("%.2f\u2013%.2f", conf_low_exp, conf_high_exp)
      ) %>%
      select(Term, OR, `95% CI`, Wald, `p-value`)
  } else {
    df <- df %>% select(Term, Estimate, SE, Wald, `p-value`)
  }

  df
}

# ─────────────────────────────────────────────────────────────────────────────
# Word document builder
# ─────────────────────────────────────────────────────────────────────────────

add_heading1 <- function(doc, text) {
  doc %>% body_add_par(text, style = "heading 1")
}

add_heading2 <- function(doc, text) {
  doc %>% body_add_par(text, style = "heading 2")
}

add_heading3 <- function(doc, text) {
  doc %>% body_add_par(text, style = "heading 3")
}

add_para <- function(doc, text) {
  doc %>% body_add_par(text, style = "Normal")
}

add_plot <- function(doc, path, width = 6, height = 3.5) {
  if (file.exists(path)) {
    doc %>% body_add_img(src = path, width = width, height = height)
  } else {
    doc %>% body_add_par(sprintf("[Figure not found: %s]", basename(path)), style = "Normal")
  }
}

add_caption <- function(doc, text) {
  fpar_obj <- fpar(
    ftext(text, fp_text(italic = TRUE, font.size = 10, color = "#444444"))
  )
  doc %>% body_add_fpar(fpar_obj)
}

add_coef_flextable <- function(doc, df) {
  ft <- flextable(df) %>%
    theme_booktabs() %>%
    fontsize(size = 9, part = "all") %>%
    bold(part = "header") %>%
    bg(i = which(str_detect(df[[ncol(df)]], "\\*")), bg = "#FFF9C4", part = "body") %>%
    autofit()
  doc %>% body_add_flextable(ft)
}

add_spacer <- function(doc) {
  doc %>% body_add_par("", style = "Normal")
}

# ─────────────────────────────────────────────────────────────────────────────
# Methods text
# ─────────────────────────────────────────────────────────────────────────────

methods_gee_text <- function() {
  list(
    overview = paste(
      "Statistical analyses were conducted using Generalized Estimating Equations (GEE),",
      "implemented in R (version 4.x) using the geepack package.",
      "GEE is a population-averaged regression approach designed for the analysis of correlated",
      "longitudinal or clustered data. Unlike mixed-effects models, which model subject-specific",
      "random effects, GEE estimates marginal effects — that is, the average effect of a predictor",
      "across the entire population of interest. This makes GEE particularly suitable for",
      "clinical studies where the primary interest is in group-level (population-averaged)",
      "treatment effects rather than individual-level predictions."
    ),
    correlation = paste(
      "An exchangeable correlation structure was specified for all models, assuming that any two",
      "repeated measurements within the same individual share an equal correlation, regardless of",
      "the time between them. This is a standard and parsimonious assumption for clinical trials",
      "with a moderate number of repeated assessments.",
      "Robust (sandwich) standard errors were used throughout, providing valid inference even",
      "when the working correlation structure is misspecified."
    ),
    models = paste(
      "Three GEE models were fitted:",
      "(1) Hemoglobin (Hb) as a continuous outcome, modelled with a Gaussian family and identity link.",
      "The model included visit number (as a categorical factor), sex, treatment group (Duroferon vs. Sideral),",
      "prior iron supplementation, age, and all two- and three-way interactions between visit, sex, and group.",
      "(2) Ferritin as a continuous outcome, with the same specification as the Hb model.",
      "(3) Side effects (binary outcome) modelled with a binomial family and logit link.",
      "Predictors included visit group (Baseline, Mid, Late), treatment group, sex, age,",
      "restless legs syndrome (RLS) status, and prior iron supplementation.",
      "For the binomial model, exponentiated coefficients (odds ratios) with 95% confidence intervals",
      "are reported."
    ),
    fit = paste(
      "Model fit was assessed using the Quasi-likelihood Information Criterion (QIC) and its variants (QICu, CIC),",
      "which are GEE analogues of the Akaike Information Criterion (AIC).",
      "Statistical significance was assessed at the conventional alpha = 0.05 threshold,",
      "with additional annotation for p < 0.01 (** ) and p < 0.001 (*** )."
    )
  )
}

methods_emmeans_text <- function() {
  paste(
    "Estimated marginal means (EMMs) were computed using the emmeans package in R.",
    "EMMs represent the model-predicted outcome for each combination of visit (or visit group),",
    "sex, and treatment group, averaged over the remaining covariates (age held at its observed mean;",
    "prior iron supplementation held at its reference level).",
    "This approach provides interpretable, covariate-adjusted predicted values for each subgroup,",
    "facilitating direct visual and statistical comparison between treatment arms across time.",
    "Pairwise contrasts between treatment groups (Duroferon vs. Sideral) were computed within each",
    "combination of sex and visit, and between visit timepoints within each sex-group combination.",
    "No multiplicity adjustment was applied to contrasts, consistent with an exploratory analysis framework."
  )
}

# ─────────────────────────────────────────────────────────────────────────────
# Build Word document
# ─────────────────────────────────────────────────────────────────────────────

build_word_report <- function(inputs, findings, cfg, project_root, out_path) {
  doc <- read_docx()
  meth <- methods_gee_text()

  # ── Title page ──────────────────────────────────────────────────────────────
  doc <- doc %>%
    add_heading1("Iron Supplementation Study — Statistical Report") %>%
    add_para(sprintf("Generated: %s", format(Sys.Date(), "%d %B %Y"))) %>%
    add_para("Status: Draft — for review only") %>%
    body_add_break()

  # ── Methods ─────────────────────────────────────────────────────────────────
  doc <- doc %>%
    add_heading1("Methods") %>%
    add_heading2("Statistical Approach: Generalized Estimating Equations") %>%
    add_para(meth$overview) %>%
    add_spacer() %>%
    add_para(meth$correlation) %>%
    add_spacer() %>%
    add_para(meth$models) %>%
    add_spacer() %>%
    add_para(meth$fit) %>%
    add_spacer() %>%
    add_heading2("Estimated Marginal Means") %>%
    add_para(methods_emmeans_text()) %>%
    body_add_break()

  # ── Results ─────────────────────────────────────────────────────────────────
  doc <- doc %>% add_heading1("Results")

  # ── 3.1 Hemoglobin ──────────────────────────────────────────────────────────
  hb_findings <- findings_for_model(findings, "hb")
  hb_narrative <- narrative_gaussian(hb_findings, "hemoglobin", "g/L")

  doc <- doc %>%
    add_heading2("3.1 Hemoglobin (Hb)") %>%
    add_para(hb_narrative) %>%
    add_spacer()

  # Observed Hb plot
  doc <- doc %>%
    add_plot(inputs$plots$hb_raw_loess, width = 6, height = 3.5) %>%
    add_caption(paste(
      "Figure 1. Observed hemoglobin (g/L) by visit and treatment group, stratified by sex.",
      "Lines represent loess-smoothed trajectories. Points show individual observations.",
      "Duroferon = orange/red; Sideral = teal."
    )) %>%
    add_spacer()

  # EMM Hb plot
  doc <- doc %>%
    add_plot(inputs$plots$hb_emmeans, width = 6, height = 3.5) %>%
    add_caption(paste(
      "Figure 2. Estimated marginal means for hemoglobin (g/L) by visit and treatment group,",
      "stratified by sex. Means are adjusted for age (at observed mean) and prior iron supplementation",
      "(at reference level). Error bars represent 95% confidence intervals.",
      "Duroferon = orange/red; Sideral = teal."
    )) %>%
    add_spacer()

  # Hb coefficient table
  doc <- doc %>%
    add_heading3("GEE Coefficients: Hemoglobin") %>%
    add_para("Table 1. GEE model coefficients for hemoglobin. Highlighted rows (yellow) indicate p < 0.05. * p<0.05, ** p<0.01, *** p<0.001.") %>%
    add_coef_flextable(make_coef_table(inputs$coefficients, "hb", is_binomial = FALSE)) %>%
    add_spacer() %>%
    body_add_break()

  # ── 3.2 Ferritin ────────────────────────────────────────────────────────────
  ferritin_findings <- findings_for_model(findings, "ferritin")
  ferritin_narrative <- narrative_gaussian(ferritin_findings, "ferritin", "\u03bcg/L")

  doc <- doc %>%
    add_heading2("3.2 Ferritin") %>%
    add_para(ferritin_narrative) %>%
    add_spacer()

  doc <- doc %>%
    add_plot(inputs$plots$ferritin_raw_loess, width = 6, height = 3.5) %>%
    add_caption(paste(
      "Figure 3. Observed ferritin (\u03bcg/L) by visit and treatment group, stratified by sex.",
      "Lines represent loess-smoothed trajectories.",
      "Duroferon = orange/red; Sideral = teal."
    )) %>%
    add_spacer()

  doc <- doc %>%
    add_plot(inputs$plots$ferritin_emmeans, width = 6, height = 3.5) %>%
    add_caption(paste(
      "Figure 4. Estimated marginal means for ferritin (\u03bcg/L) by visit and treatment group,",
      "stratified by sex. Adjusted for age and prior iron supplementation.",
      "Error bars represent 95% confidence intervals.",
      "Duroferon = orange/red; Sideral = teal."
    )) %>%
    add_spacer()

  doc <- doc %>%
    add_heading3("GEE Coefficients: Ferritin") %>%
    add_para("Table 2. GEE model coefficients for ferritin. Highlighted rows (yellow) indicate p < 0.05. * p<0.05, ** p<0.01, *** p<0.001.") %>%
    add_coef_flextable(make_coef_table(inputs$coefficients, "ferritin", is_binomial = FALSE)) %>%
    add_spacer() %>%
    body_add_break()

  # ── 3.3 Side Effects ────────────────────────────────────────────────────────
  se_findings <- findings_for_model(findings, "side_effect")
  se_narrative <- narrative_binomial(se_findings, "side effects")

  doc <- doc %>%
    add_heading2("3.3 Side Effects") %>%
    add_para(se_narrative) %>%
    add_spacer()

  doc <- doc %>%
    add_plot(inputs$plots$side_effect_emmeans, width = 6, height = 3.5) %>%
    add_caption(paste(
      "Figure 5. Predicted side-effect probability by visit group and treatment group,",
      "stratified by sex. Predictions are adjusted for age (at observed mean), RLS status",
      "(at reference level), and prior iron supplementation (at reference level).",
      "Error bars represent 95% confidence intervals.",
      "Duroferon = orange/red; Sideral = teal.",
      "Note: visit groups are ordered Baseline \u2192 Mid \u2192 Late on the x-axis."
    )) %>%
    add_spacer()

  doc <- doc %>%
    add_heading3("GEE Coefficients: Side Effects (Odds Ratios)") %>%
    add_para("Table 3. GEE model odds ratios for side effects. Highlighted rows (yellow) indicate p < 0.05. * p<0.05, ** p<0.01, *** p<0.001. OR = odds ratio. 95% CI = confidence interval.") %>%
    add_coef_flextable(make_coef_table(inputs$coefficients, "side_effect", is_binomial = TRUE)) %>%
    add_spacer()

  # ── Save ────────────────────────────────────────────────────────────────────
  print(doc, target = out_path)
  message("Word report written: ", out_path)
  out_path
}

# ─────────────────────────────────────────────────────────────────────────────
# Build HTML report
# ─────────────────────────────────────────────────────────────────────────────

build_html_report <- function(inputs, findings, cfg, project_root, out_path) {

  encode_image <- function(path) {
    if (!file.exists(path)) return(NULL)
    b64 <- base64enc::base64encode(path)
    sprintf('<img src="data:image/png;base64,%s" style="max-width:100%%;margin:12px 0;">', b64)
  }

  p_tag  <- function(text) sprintf("<p>%s</p>", text)
  h1_tag <- function(text) sprintf("<h1>%s</h1>", text)
  h2_tag <- function(text) sprintf("<h2>%s</h2>", text)
  h3_tag <- function(text) sprintf("<h3>%s</h3>", text)
  caption_tag <- function(text) sprintf('<p class="caption"><em>%s</em></p>', text)
  hr_tag <- function() "<hr>"

  table_html <- function(df) {
    header <- paste(
      "<thead><tr>",
      paste(sprintf("<th>%s</th>", names(df)), collapse = ""),
      "</tr></thead>"
    )
    rows <- apply(df, 1, function(row) {
      sig <- any(str_detect(row, "\\*"))
      tr_class <- if (sig) ' class="sig"' else ""
      paste0(
        sprintf("<tr%s>", tr_class),
        paste(sprintf("<td>%s</td>", row), collapse = ""),
        "</tr>"
      )
    })
    paste0("<table>", header, "<tbody>", paste(rows, collapse = ""), "</tbody></table>")
  }

  css <- "
    <style>
      body { font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px;
             color: #222; line-height: 1.7; }
      h1 { color: #1a3a5c; border-bottom: 2px solid #1a3a5c; padding-bottom: 6px; margin-top: 40px; }
      h2 { color: #2c5f8a; margin-top: 30px; }
      h3 { color: #3a7ab5; margin-top: 20px; }
      p  { margin: 10px 0; }
      .caption { color: #555; font-size: 0.9em; margin-top: 4px; border-left: 3px solid #ccc;
                 padding-left: 10px; }
      table { border-collapse: collapse; width: 100%; margin: 16px 0; font-size: 0.88em; }
      th { background: #1a3a5c; color: white; padding: 8px 10px; text-align: left; }
      td { padding: 6px 10px; border-bottom: 1px solid #ddd; }
      tr.sig { background: #fffde7; }
      tr:hover { background: #f0f4ff; }
      .draft-banner { background: #fff3cd; border: 1px solid #ffc107; padding: 10px 16px;
                      border-radius: 4px; margin-bottom: 20px; font-size: 0.9em; }
      hr { border: none; border-top: 1px solid #ccc; margin: 30px 0; }
      img { border: 1px solid #ddd; border-radius: 4px; }
      .legend { font-size: 0.85em; color: #555; }
    </style>
  "

  meth <- methods_gee_text()
  hb_findings       <- findings_for_model(findings, "hb")
  ferritin_findings <- findings_for_model(findings, "ferritin")
  se_findings       <- findings_for_model(findings, "side_effect")

  lines <- c(
    "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>",
    "<title>Iron Study Statistical Report</title>",
    css,
    "</head><body>",

    h1_tag("Iron Supplementation Study &mdash; Statistical Report"),
    sprintf('<div class="draft-banner">&#9888; Draft report &mdash; generated %s. For review only.</div>', format(Sys.Date(), "%d %B %Y")),

    # Methods
    h1_tag("1. Methods"),
    h2_tag("1.1 Generalized Estimating Equations"),
    p_tag(meth$overview),
    p_tag(meth$correlation),
    p_tag(meth$models),
    p_tag(meth$fit),
    h2_tag("1.2 Estimated Marginal Means"),
    p_tag(methods_emmeans_text()),
    hr_tag(),

    # Results
    h1_tag("2. Results"),

    # Hb
    h2_tag("2.1 Hemoglobin (Hb)"),
    p_tag(narrative_gaussian(hb_findings, "hemoglobin", "g/L")),
    encode_image(inputs$plots$hb_raw_loess) %||% "",
    caption_tag("Figure 1. Observed hemoglobin (g/L) by visit and treatment group, stratified by sex. Lines represent loess-smoothed trajectories. Duroferon = orange/red; Sideral = teal."),
    encode_image(inputs$plots$hb_emmeans) %||% "",
    caption_tag("Figure 2. Estimated marginal means for hemoglobin (g/L) by visit and treatment group, stratified by sex. Adjusted for age and prior iron supplementation. Error bars = 95% CI."),
    h3_tag("GEE Coefficients: Hemoglobin"),
    p_tag("Table 1. GEE model coefficients for hemoglobin. Highlighted rows indicate p &lt; 0.05. * p&lt;0.05, ** p&lt;0.01, *** p&lt;0.001."),
    table_html(make_coef_table(inputs$coefficients, "hb", is_binomial = FALSE)),
    hr_tag(),

    # Ferritin
    h2_tag("2.2 Ferritin"),
    p_tag(narrative_gaussian(ferritin_findings, "ferritin", "&mu;g/L")),
    encode_image(inputs$plots$ferritin_raw_loess) %||% "",
    caption_tag("Figure 3. Observed ferritin (&mu;g/L) by visit and treatment group, stratified by sex. Lines represent loess-smoothed trajectories. Duroferon = orange/red; Sideral = teal."),
    encode_image(inputs$plots$ferritin_emmeans) %||% "",
    caption_tag("Figure 4. Estimated marginal means for ferritin (&mu;g/L) by visit and treatment group, stratified by sex. Adjusted for age and prior iron supplementation. Error bars = 95% CI."),
    h3_tag("GEE Coefficients: Ferritin"),
    p_tag("Table 2. GEE model coefficients for ferritin. Highlighted rows indicate p &lt; 0.05. * p&lt;0.05, ** p&lt;0.01, *** p&lt;0.001."),
    table_html(make_coef_table(inputs$coefficients, "ferritin", is_binomial = FALSE)),
    hr_tag(),

    # Side effects
    h2_tag("2.3 Side Effects"),
    p_tag(narrative_binomial(se_findings, "side effects")),
    encode_image(inputs$plots$side_effect_emmeans) %||% "",
    caption_tag("Figure 5. Predicted side-effect probability by visit group and treatment group, stratified by sex. Adjusted for age, RLS status, and prior iron supplementation. Error bars = 95% CI. Visit groups ordered Baseline &rarr; Mid &rarr; Late."),
    h3_tag("GEE Coefficients: Side Effects (Odds Ratios)"),
    p_tag("Table 3. GEE model odds ratios for side effects. Highlighted rows indicate p &lt; 0.05. * p&lt;0.05, ** p&lt;0.01, *** p&lt;0.001. OR = odds ratio, 95% CI = confidence interval."),
    table_html(make_coef_table(inputs$coefficients, "side_effect", is_binomial = TRUE)),

    "</body></html>"
  )

  writeLines(lines, out_path, useBytes = TRUE)
  message("HTML report written: ", out_path)
  out_path
}

# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

run_fil7 <- function(config_path = "config.yaml") {
  config_path  <- normalizePath(config_path, mustWork = TRUE)
  project_root <- dirname(config_path)
  cfg          <- yaml::read_yaml(config_path)

  # Check for base64enc (needed for HTML image embedding)
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Install with: install.packages('base64enc')", call. = FALSE)
  }

  out_dir <- ensure_dir(file.path(
    project_root,
    get_cfg(cfg, "fil7.output.dir", default = "outputs/fil7")
  ))

  docx_filename <- get_cfg(cfg, "fil7.output.docx_filename", default = "statistical_report.docx")
  html_filename <- get_cfg(cfg, "fil7.output.html_filename", default = "statistical_report.html")

  docx_path <- file.path(out_dir, docx_filename)
  html_path <- file.path(out_dir, html_filename)

  alpha <- get_cfg(cfg, "fil7.significance.alpha", default = 0.05)

  message("Reading inputs...")
  inputs <- read_all_inputs(cfg, project_root)

  message("Extracting findings (alpha = ", alpha, ")...")
  findings <- extract_findings(inputs$coefficients, alpha = alpha)

  message("Building Word report...")
  build_word_report(inputs, findings, cfg, project_root, docx_path)

  message("Building HTML report...")
  build_html_report(inputs, findings, cfg, project_root, html_path)

  list(
    paths = list(
      docx = docx_path,
      html = html_path
    ),
    findings = findings
  )
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  config_path <- if (length(args) >= 1) args[[1]] else "config.yaml"
  out <- run_fil7(config_path)
  message("FIL 7 finished.")
  message("Word: ", out$paths$docx)
  message("HTML: ", out$paths$html)
}
