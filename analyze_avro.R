# Load libraries
tryCatch({
  required_pkgs <- c("sparklyr", "dplyr", "data.table", "purrr", "tibble",
          "writexl", "ggplot2", "rlang", "survival", "survminer", "ggpubr", "tidyr")
  
  install.packages(setdiff(required_pkgs, rownames(installed.packages())))
  
  invisible(lapply(required_pkgs, library, character.only = TRUE))
}, error = function(e) {
  message("An error occurred: ", e$message)
})

# Connect to Spark
tryCatch({
  sc <- spark_connect(
    master = "local",
    spark_home = "/opt/spark-3.5.0",
    packages = c("org.apache.spark:spark-avro_2.12:3.5.0")
  )
}, error = function(e) {
  stop("Failed to connect to Spark: ", e$message)
})

# Read AVRO File
tryCatch({
  # Example: set this to your actual file name
  avro_file_path <- "/home/rstudio/file_path.avro"
  spark_df <- spark_read_avro(sc, name = "avro_data", path = avro_file_path)
}, error = function(e) {
  stop("Failed to read AVRO file: ", e$message)
})

# Get All Entity Types
tryCatch({
  entity_types <- spark_df %>%
    distinct(name) %>%
    collect() %>%
    pull(name)
  
  dfs <- map(entity_types, ~ spark_df %>% filter(name == .x)) %>%
    set_names(entity_types)
}, error = function(e) {
  stop("Failed to extract entity types: ", e$message)
})

# Flatten Function
flatten_entity_df <- function(entity_sdf) {
  tryCatch({
    df_local <- entity_sdf %>% collect()
    
    flattened_rows <- map(1:nrow(df_local), function(i) {
      row <- df_local[i, ]
      top_name <- row$name
      obj_raw <- row$object[[1]]
      obj <- if (length(obj_raw) > 0) obj_raw[[1]] else list()
      
      if (!is.null(obj$state) && is.list(obj$state) && !is.null(obj$state$member0)) {
        obj$state <- obj$state$member0
      }
      
      if (!is.null(obj$name)) {
        obj[[paste0(top_name, "_name")]] <- obj$name
        obj$name <- NULL
      }
      
      obj_df <- tryCatch({
        if (is.atomic(obj)) tibble() else as_tibble(obj)
      }, error = function(e) tibble())
      
      relation <- if (length(row$relations[[1]]) > 0) row$relations[[1]][[1]] else NULL
      rel_df <- tryCatch(
        if (!is.null(relation)) tibble(dst_id = relation$dst_id, dst_name = relation$dst_name)
        else tibble(dst_id = NA, dst_name = NA),
        error = function(e) tibble(dst_id = NA, dst_name = NA)
      )
      
      bind_cols(
        tibble(id = row$id, name = top_name),
        obj_df,
        rel_df
      )
    })
    
    final_df <- bind_rows(flattened_rows)
    
    final_df[] <- lapply(final_df, function(col) {
      if (is.character(col)) {
        col <- gsub("_2d", "-", col, fixed = TRUE)
        col <- gsub("_2f", "/", col, fixed = TRUE)
        col <- gsub("_20", " ", col, fixed = TRUE)
        col <- gsub(" _", " ", col, fixed = TRUE)
        col <- gsub(" +", " ", col)
        col <- gsub("-_", "-", col)
        col <- gsub("- ", "-", col)
        col <- gsub("_28_", "(", col)
        col <- gsub("_2e_", ".", col)
        col <- gsub("_29_", ")", col)
        col <- gsub("/_", "_", col)
        col <- trimws(col)
      }
      col
    })
    
    final_df[is.na(final_df)] <- "None"
    cat("Flattened", nrow(final_df), "records for", unique(df_local$name), "\n")
    return(final_df)
  }, error = function(e) {
    message("Error flattening entity: ", e$message)
    return(tibble())
  })
}

# Flatten and Export Entities
tryCatch({
  timing_df <- flatten_entity_df(dfs[["timing"]])
  # write_xlsx(timing_df, "timing_flattened.xlsx")
  
  subject_df <- flatten_entity_df(dfs[["subject"]])
  # write_xlsx(subject_df, "subject_flattened.xlsx")
  
  tumor_assessment_df <- flatten_entity_df(dfs[["tumor_assessment"]])
  # write_xlsx(tumor_assessment_df, "tumor_assessment_flattened.xlsx")
  
  histology_df <- flatten_entity_df(dfs[["histology"]])
  # write_xlsx(histology_df, "histology_flattened.xlsx")
  
  lab_df <- flatten_entity_df(dfs[["lab"]])
  # write_xlsx(lab_df, "lab_flattened.xlsx")
  
  total_dose_df <- flatten_entity_df(dfs[["total_dose"]])
  # write_xlsx(total_dose_df, "total_dose_flattened.xlsx")
  
  survival_characteristic_df <- flatten_entity_df(dfs[["survival_characteristic"]])
  # write_xlsx(survival_characteristic_df, "survival_characteristic_flattened.xlsx")
}, error = function(e) {
  stop("Error flattening or writing entities: ", e$message)
})

# Merge Operations
tryCatch({
  merged_df <- timing_df %>%
    inner_join(subject_df, by = c("dst_id" = "id"), suffix = c("_timing", "_subject"))
  # write_xlsx(merged_df, "timing_subject_merged.xlsx")
  
  merged_df <- merged_df %>%
    inner_join(tumor_assessment_df, by = "dst_id", suffix = c("", "_tumor"))
  # write_xlsx(merged_df, "timing_subject_tumor_merged.xlsx")
  
  merged_df <- merged_df %>%
    inner_join(histology_df, by = "dst_id", suffix = c("", "_histology"))
  # write_xlsx(merged_df, "timing_subject_tumor_histology_merged.xlsx")
  
  merged_df <- merged_df %>%
    inner_join(lab_df, by = "dst_id", suffix = c("", "_lab"))
  # write_xlsx(merged_df, "timing_subject_tumor_histology_lab_merged.xlsx")
  
  totaldose_selected <- total_dose_df %>%
    select(dst_id, antineoplastic_agent)
  
  merged_df <- merged_df %>%
    left_join(totaldose_selected, by = "dst_id", suffix = c("", "_total_dose"))
  # write_xlsx(merged_df, "timing_subject_tumor_histology_lab_totaldose_merged.xlsx")
}, error = function(e) {
  stop("Error during merging operations: ", e$message)
})

# Plot Value Distribution
suppressWarnings(
  tryCatch({
    # Change this value as needed
    column_name <- "censor_status"
    
    value_percentages <- merged_df %>%
      count(!!sym(column_name)) %>%
      mutate(
        Percentage = round((n / sum(n)) * 100, 2),
        Label = paste0(Percentage, "%")
      )
    
    write_xlsx(value_percentages %>% select(!!sym(column_name), Percentage), "value_percentages.xlsx")
    
    ggplot(value_percentages, aes(x = reorder(!!sym(column_name), -Percentage), y = Percentage, fill = !!sym(column_name))) +
      geom_bar(stat = "identity", width = 0.7) +
      geom_text(aes(label = Label), vjust = -0.5) +
      labs(
        title = paste("Distribution of", column_name),
        x = column_name,
        y = "Percentage (%)"
      ) +
      theme_minimal() +
      theme(legend.position = "none")
  }, error = function(e) {
    message("Error generating plot: ", e$message)
  })
)

# Survival Curves
# 1. Overall Survival Curve
tryCatch({
  diagnosis_age_df <- timing_df %>%
    filter(disease_phase == "Initial Diagnosis") %>%
    select(dst_id, age_at_disease_phase) %>%
    distinct()
}, error = function(e) {
  message("ERROR extracting diagnosis age: ", conditionMessage(e))
})

tryCatch({
  survival_data <- survival_characteristic_df %>%
    select(dst_id, age_at_lkss, lkss) %>%
    inner_join(diagnosis_age_df, by = "dst_id") %>%
    mutate(
      time_years = (age_at_lkss - age_at_disease_phase) / 365.25,
      event = ifelse(lkss == "Dead", 1, 0)
    ) %>%
    filter(
      !is.na(time_years),
      !is.na(event),
      time_years >= 0
    )
}, error = function(e) {
  message("ERROR preparing Overall Survival data: ", conditionMessage(e))
})

tryCatch({
  km_fit_overall <- survfit(Surv(time_years, event) ~ 1, data = survival_data)
  ggsurvplot(
    km_fit_overall,
    conf.int = TRUE,
    risk.table = TRUE,
    risk.table.col = "black",
    risk.table.height = 0.25,
    surv.median.line = "hv",
    xlab = "Time Since Diagnosis (Years)",
    ylab = "Overall Survival Probability",
    title = "Kaplan-Meier Overall Survival Curve",
    palette = "Dark2",
    ggtheme = theme_minimal(base_size = 14)
  )
}, error = function(e) {
  message("ERROR fitting or plotting Overall Survival: ", conditionMessage(e))
})

# 2. Event-Free Survival Curve
tryCatch({
  efs_data <- subject_df %>%
    select(id, age_at_censor_status, censor_status) %>%
    inner_join(diagnosis_age_df, by = c("id" = "dst_id")) %>%
    mutate(
      time_years = (age_at_censor_status - age_at_disease_phase) / 365.25,
      event = ifelse(censor_status == "Subject has had one or more events", 1, 0)
    ) %>%
    filter(
      !is.na(time_years),
      !is.na(event),
      time_years >= 0
    )
}, error = function(e) {
  message("ERROR preparing Event-Free Survival data: ", conditionMessage(e))
})

tryCatch({
  km_fit_efs <- survfit(Surv(time_years, event) ~ 1, data = efs_data)
  ggsurvplot(
    km_fit_efs,
    conf.int = TRUE,
    risk.table = TRUE,
    risk.table.col = "black",
    risk.table.height = 0.25,
    surv.median.line = "hv",
    xlab = "Time Since Diagnosis (Years)",
    ylab = "Event-Free Survival Probability",
    title = "Kaplan-Meier Event-Free Survival Curve",
    palette = "Dark2",
    ggtheme = theme_minimal(base_size = 14)
  )
}, error = function(e) {
  message("ERROR fitting or plotting Event-Free Survival: ", conditionMessage(e))
})

tryCatch({
  write_xlsx(survival_data, "overall_survival_data.xlsx")
  write_xlsx(efs_data, "event_free_survival_data.xlsx")
}, error = function(e) {
  message("ERROR writing Excel files: ", conditionMessage(e))
})

# Flatten and prepare SMN data
tryCatch({
  person_df <- flatten_entity_df(dfs[["person"]])
  secondary_malignant_neoplasm_df <- flatten_entity_df(dfs[["secondary_malignant_neoplasm"]])
}, error = function(e) {
  message("Error flattening entity data: ", e$message)
})

# Filter SMN Yes/No
tryCatch({
  smn_df <- secondary_malignant_neoplasm_df %>%
    filter(smn_yn %in% c("Yes", "No")) %>%
    select(dst_id, smn_yn) %>%
    distinct()
}, error = function(e) {
  message("Error filtering SMN data: ", e$message)
})

# Merge with subject_df
tryCatch({
  summary_df <- merge(
    smn_df,
    subject_df[, c("id", "dst_id")],
    by.x = "dst_id",
    by.y = "id",
    all.x = TRUE,
    suffixes = c("", "_subject")
  )
}, error = function(e) {
  message("Error merging with subject_df: ", e$message)
})

# Merge with person_df
tryCatch({
  summary_df <- merge(
    summary_df,
    person_df,
    by.x = "dst_id_subject",
    by.y = "id",
    all.x = TRUE,
    suffixes = c("", "_person")
  )
}, error = function(e) {
  message("Error merging with person_df: ", e$message)
})

# Filter timing for Initial Diagnosis
tryCatch({
  timing_filtered <- timing_df %>%
    filter(disease_phase == "Initial Diagnosis") %>%
    select(dst_id, age_at_disease_phase) %>%
    distinct(dst_id, .keep_all = TRUE)
}, error = function(e) {
  message("Error filtering timing_df: ", e$message)
})

# Merge with timing
tryCatch({
  smn_analysis_df <- merge(
    summary_df,
    timing_filtered,
    by = "dst_id",
    all.x = TRUE,
    suffixes = c("", "_timing")
  )
}, error = function(e) {
  message("Error merging with timing_df: ", e$message)
})

# Export final summary
tryCatch({
  write_xlsx(smn_analysis_df, "smn_analysis.xlsx")
}, error = function(e) {
  message("Error exporting final summary: ", e$message)
})

# Prepare age_df
tryCatch({
  age_df <- smn_analysis_df %>%
    filter(!is.na(age_at_disease_phase))
}, error = function(e) {
  message("Error preparing age_df: ", e$message)
})

# Calculate means
tryCatch({
  mean_age_yes <- age_df %>%
    filter(smn_yn == "Yes") %>%
    summarise(mean = mean(age_at_disease_phase)) %>%
    pull(mean)

  mean_age_no <- age_df %>%
    filter(smn_yn == "No") %>%
    summarise(mean = mean(age_at_disease_phase)) %>%
    pull(mean)
}, error = function(e) {
  message("Error calculating mean age: ", e$message)
})

# Add age groups
tryCatch({
  age_df <- age_df %>%
    mutate(age_group = ifelse(age_at_disease_phase < 18, "< 18 mo", ">= 18 mo"))
}, error = function(e) {
  message("Error assigning age groups: ", e$message)
})

# Age group summary
age_group_summary <- tryCatch({
  age_df %>%
    group_by(smn_yn, age_group) %>%
    summarise(n = n(), .groups = "drop") %>%
    group_by(smn_yn) %>%
    mutate(percentage = round(100 * n / sum(n), 1)) %>%
    select(smn_yn, age_group, percentage) %>%
    pivot_wider(names_from = smn_yn, values_from = percentage)
}, error = function(e) {
  message("Error summarizing age groups: ", e$message)
  tibble(age_group = character(), Yes = numeric(), No = numeric())
})

# Sex summary
sex_summary <- tryCatch({
  smn_analysis_df %>%
    filter(sex %in% c("Male", "Female")) %>%
    group_by(smn_yn, sex) %>%
    summarise(n = n(), .groups = "drop") %>%
    group_by(smn_yn) %>%
    mutate(percentage = round(100 * n / sum(n), 1)) %>%
    select(smn_yn, sex, percentage) %>%
    pivot_wider(names_from = smn_yn, values_from = percentage)
}, error = function(e) {
  message("Error summarizing sex distribution: ", e$message)
  tibble(sex = character(), Yes = numeric(), No = numeric())
})

# Labels
tryCatch({
  sample_label_age <- paste0(nrow(age_df), " (", sum(age_df$smn_yn == "Yes"), ")")
  sample_label_sex <- paste0(nrow(smn_analysis_df), " (", sum(smn_analysis_df$smn_yn == "Yes"), ")")
}, error = function(e) {
  message("Error creating sample size labels: ", e$message)
})

# Summary table
tryCatch({
  summary_table <- tibble::tibble(
    `Co-variate` = c(
      "Mean age at diagnosis (mo)",
      "Age at diagnosis",
      "< 18 (mo)",
      ">= 18 (mo)",
      "Sex",
      "Female",
      "Male"
    ),
    `Sample Size (SMN)` = c(
      sample_label_age,
      sample_label_age,
      "",
      "",
      sample_label_sex,
      "",
      ""
    ),
    `SMN Formed` = c(
      round(mean_age_yes, 1),
      "",
      age_group_summary$Yes[age_group_summary$age_group == "< 18 mo"],
      age_group_summary$Yes[age_group_summary$age_group == ">= 18 mo"],
      "",
      sex_summary$Yes[sex_summary$sex == "Female"],
      sex_summary$Yes[sex_summary$sex == "Male"]
    ),
    `No SMN Formed` = c(
      round(mean_age_no, 1),
      "",
      age_group_summary$No[age_group_summary$age_group == "< 18 mo"],
      age_group_summary$No[age_group_summary$age_group == ">= 18 mo"],
      "",
      sex_summary$No[sex_summary$sex == "Female"],
      sex_summary$No[sex_summary$sex == "Male"]
    )
  )

  summary_table_clean <- summary_table %>%
    mutate(across(everything(), ~ ifelse(is.na(.), "", as.character(.))))
}, error = function(e) {
  message("Error creating summary table: ", e$message)
})

# Statistical tests
tryCatch({
  p_age <- t.test(age_at_disease_phase ~ smn_yn, data = age_df)$p.value
  p_age_group <- fisher.test(table(age_df$age_group, age_df$smn_yn))$p.value

  sex_filtered <- smn_analysis_df %>%
    filter(sex %in% c("Male", "Female"))

  sex_table <- table(sex_filtered$sex, sex_filtered$smn_yn)
  p_sex <- chisq.test(sex_table)$p.value
}, error = function(e) {
  message("Error performing statistical tests: ", e$message)
})

# Add p-values
tryCatch({
  summary_table_clean <- summary_table_clean %>%
    mutate(`p-value` = c(
      sprintf("%.3f", p_age),
      "",
      sprintf("%.3f", p_age_group),
      "",
      "",
      sprintf("%.3f", p_sex),
      ""
    ))
}, error = function(e) {
  message("Error adding p-values to summary: ", e$message)
})

# Display table
tryCatch({
  gg_table <- ggtexttable(
    summary_table_clean,
    rows = NULL,
    theme = ttheme("classic", base_size = 16)
  )
  print(gg_table)
}, error = function(e) {
  message("Error displaying summary table: ", e$message)
})

# Alternative dynamic code to calculate the percentage of SMN and non-SMN for a given covariate.
# If we have N covariates, we can use this function to calculate the percentages for each.
summarize_selected_categorical_by_smn <- function(data, selected_columns = NULL, values_to_ignore = list()) {

  if (is.null(selected_columns)) {
    stop("Please provide a vector of column names in selected_columns.")
  }
  
  results <- list()
  
  for (col in selected_columns) {
    
    if (!col %in% colnames(data)) {
      warning(paste("Column", col, "not found. Skipping."))
      next
    }
    
    clean_data <- data %>%
      filter(!is.na(smn_yn), !is.na(.data[[col]])) %>%
      distinct(dst_id, smn_yn, .data[[col]])
    
    if (!is.null(values_to_ignore[[col]])) {
      clean_data <- clean_data %>%
        filter(!.data[[col]] %in% values_to_ignore[[col]])
    }
    
    yes_data <- clean_data %>% filter(smn_yn == "Yes")
    no_data  <- clean_data %>% filter(smn_yn == "No")
    
    yes_summary <- yes_data %>%
      group_by(.data[[col]]) %>%
      summarise(n = n(), .groups = "drop") %>%
      mutate(percentage_yes = 100 * n / sum(n))
    
    no_summary <- no_data %>%
      group_by(.data[[col]]) %>%
      summarise(n = n(), .groups = "drop") %>%
      mutate(percentage_no = 100 * n / sum(n))
    
    merged_summary <- full_join(
      yes_summary %>% select(!!col, percentage_yes),
      no_summary %>% select(!!col, percentage_no),
      by = col
    ) %>%
      arrange(.data[[col]]) %>%
      replace_na(list(percentage_yes = 0, percentage_no = 0))
    
    results[[col]] <- merged_summary
  }
  
  return(results)
}

# Example
results <- summarize_selected_categorical_by_smn(
  data = smn_analysis_df,
  selected_columns = c("race", "sex", "ethnicity"),
  values_to_ignore = list(
    sex = c("Other", "Unknown", "Undifferentiated", "None", "Not Reported"),
    race = c( "Otheasr")
  )
)

print(results$sex)