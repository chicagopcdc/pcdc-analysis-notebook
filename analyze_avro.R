# Load libraries
tryCatch({
  required_pkgs <- c("sparklyr", "dplyr", "data.table", "purrr", "tibble",
          "writexl", "ggplot2", "rlang", "survival", "survminer")
  
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