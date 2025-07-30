# Load libraries
tryCatch({
  required_pkgs <- c("sparklyr", "dplyr", "data.table", "purrr", "tibble",
                     "writexl", "ggplot2", "rlang", "survival", "survminer", "ggpubr", "tidyr", "shiny", "shinythemes")
  
  install.packages(setdiff(required_pkgs, rownames(installed.packages())))
  
  invisible(lapply(required_pkgs, library, character.only = TRUE))
}, error = function(e) {
  message("An error occurred: ", e$message)
})

# Set max upload size(50 MB)
options(shiny.maxRequestSize = 50 * 1024^2)

saveUploadedFile <- function(fileInput) {
  if (is.null(fileInput)) return(NULL)
  
  original_name <- fileInput$name
  temp_path <- fileInput$datapath
  target_path <- file.path("/home/rstudio", original_name)
  
  file.copy(temp_path, target_path, overwrite = TRUE)
  return(target_path)
}

ui <- fluidPage(
  theme = shinytheme("flatly"),
  titlePanel("AVRO File Analysis"),
  
  sidebarLayout(
    sidebarPanel(
      h4("Upload AVRO File"),
      fileInput("avroFile", "Choose AVRO File:", accept = ".avro"),
      textOutput("filePath"),
      hr(),
      
      h4("Column Distribution"),
      textInput("col_input", "Enter Column Name:"),
      actionButton("submit", "Generate Distribution Plot", class = "btn-info"),
      hr(),
      
      h4("Survival Curves"),
      p("Overall Survival"),
      actionButton("overallSurvivalBtn", "Show Overall Survival Curve", class = "btn-info"),
      br(), br(),
      p("Event-Free Survival"),
      actionButton("eventFreeSurvivalBtn", "Show Event-Free Survival Curve", class = "btn-info"),
      hr(),
      
      h4("Covariate Selection"),
      checkboxGroupInput("covariates", "Select Covariates:", choices = paste0("Covariate ", 1:10)),
      actionButton("summaryBtn", "Plot Summary Table", class = "btn-info"),
      hr(),
      
      verbatimTextOutput("errorMsg")
    ),
    
    mainPanel(
      tabsetPanel(
        id = "tabs",
        tabPanel("Distribution Plot", plotOutput("percentagePlot", height = "500px")),
        tabPanel("Overall Survival", plotOutput("overallSurvivalPlot", height = "500px")),
        tabPanel("Event-Free Survival", plotOutput("eventFreeSurvivalPlot", height = "500px")),
        tabPanel("Summary Table", tableOutput("summaryTable"))
      )
    )
  )
)

server <- function(input, output, session) {
  
  # Display saved file path
  output$filePath <- renderText({
    path <- savedFilePath()
    if (is.null(path)) {
      "No file uploaded."
    } else {
      paste("File saved to:", path)
    }
  })
  
  savedFilePath <- reactiveVal(NULL)
  
  # File uplode event
 observeEvent(input$avroFile, {
  req(input$avroFile)
  output$errorMsg <- renderText({ "Uploading and processing AVRO file..." })
  
  path <- saveUploadedFile(input$avroFile)
  savedFilePath(path)

  withProgress(message = "Processing AVRO file", value = 0, {
    tryCatch({
      incProgress(0.1, detail = "Connecting to Spark...")
      sc <<- spark_connect(
        master = "local",
        spark_home = "/opt/spark-3.5.0",
        packages = c("org.apache.spark:spark-avro_2.12:3.5.0")
      )
      
      incProgress(0.1, detail = "Reading AVRO into Spark...")
      spark_df <<- spark_read_avro(sc, name = "avro_data", path = path)
      
      incProgress(0.1, detail = "Extracting entity types...")
      entity_types <- spark_df %>% distinct(name) %>% collect() %>% pull(name)
      dfs <<- map(entity_types, ~ spark_df %>% filter(name == .x)) %>% set_names(entity_types)
      
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
          output$errorMsg <- renderText({
            paste("Flattened", nrow(final_df), "records for", unique(df_local$name))
          })
          return(final_df)
        }, error = function(e) {
          message("Error flattening entity: ", e$message)
          return(tibble())
        })
      }

      incProgress(0.2, detail = "Flattening entities...")
      timing_df <<- flatten_entity_df(dfs[["timing"]])
      subject_df <<- flatten_entity_df(dfs[["subject"]])
      tumor_assessment_df <<- flatten_entity_df(dfs[["tumor_assessment"]])
      histology_df <<- flatten_entity_df(dfs[["histology"]])
      lab_df <<- flatten_entity_df(dfs[["lab"]])
      total_dose_df <<- flatten_entity_df(dfs[["total_dose"]])
      survival_characteristic_df <<- flatten_entity_df(dfs[["survival_characteristic"]])
      
      incProgress(0.3, detail = "Merging data...")
      merged_df <<- timing_df %>%
        inner_join(subject_df, by = c("dst_id" = "id"), suffix = c("_timing", "_subject")) %>%
        inner_join(tumor_assessment_df, by = "dst_id", suffix = c("", "_tumor")) %>%
        inner_join(histology_df, by = "dst_id", suffix = c("", "_histology")) %>%
        inner_join(lab_df, by = "dst_id", suffix = c("", "_lab"))
      
      totaldose_selected <- total_dose_df %>% select(dst_id, antineoplastic_agent)
      merged_df <<- merged_df %>% left_join(totaldose_selected, by = "dst_id", suffix = c("", "_total_dose"))

      output$errorMsg <- renderText({
        paste0("Processing complete. Total merged records: ", nrow(merged_df))
      })
      
    }, error = function(e) {
      output$errorMsg <- renderText({ paste("Error during processing:", e$message) })
    })
  })
})

  observeEvent(input$submit, {
    req(input$col_input)
    
    tryCatch({
      column_name <- input$col_input
      
      if (!column_name %in% colnames(merged_df)) {
        stop("Column not found in the dataset.")
      }
      
      value_percentages <- merged_df %>%
        count(!!sym(column_name)) %>%
        mutate(
          Percentage = round((n / sum(n)) * 100, 2),
          Label = paste0(Percentage, "%")
        )
      
      write_xlsx(value_percentages %>% select(!!sym(column_name), Percentage), "value_percentages.xlsx")
      
      output$percentagePlot <- renderPlot({
        ggplot(value_percentages, aes(x = reorder(!!sym(column_name), -Percentage), y = Percentage, fill = !!sym(column_name))) +
          geom_bar(stat = "identity", width = 0.7) +
          geom_text(aes(label = Label), vjust = -0.5) +
          labs(
            title = paste("Distribution of", column_name),
            x = column_name,
            y = "Percentage (%)"
          ) +
          theme_minimal(base_size = 14) +
          theme(legend.position = "none")
      }, res = 120)
      
      output$errorMsg <- renderText({ "" })
      
    }, error = function(e) {
      output$percentagePlot <- renderPlot({ NULL })
      output$errorMsg <- renderText({ paste("Error:", e$message) })
    })
  })
  observeEvent(input$overallSurvivalBtn, {
    tryCatch({
      diagnosis_age_df <<- timing_df %>%
        filter(disease_phase == "Initial Diagnosis") %>%
        select(dst_id, age_at_disease_phase) %>%
        distinct()
      
      survival_data <<- survival_characteristic_df %>%
        select(dst_id, age_at_lkss, lkss) %>%
        inner_join(diagnosis_age_df, by = "dst_id") %>%
        mutate(
          time_years = (age_at_lkss - age_at_disease_phase) / 365.25,
          event = ifelse(lkss == "Dead", 1, 0)
        ) %>%
        filter(!is.na(time_years), !is.na(event), time_years >= 0)
      
      km_fit_overall <<- survfit(Surv(time_years, event) ~ 1, data = survival_data)
      
      output$overallSurvivalPlot <- renderPlot({
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
      })
      
      updateTabsetPanel(session, "tabs", selected = "Overall Survival")
      
    }, error = function(e) {
      output$errorMsg <- renderText({ paste("ERROR:", conditionMessage(e)) })
    })
  })
  
  observeEvent(input$eventFreeSurvivalBtn, {
    tryCatch({
      efs_data <<- subject_df %>%
        select(id, age_at_censor_status, censor_status) %>%
        inner_join(diagnosis_age_df, by = c("id" = "dst_id")) %>%
        mutate(
          time_years = (age_at_censor_status - age_at_disease_phase) / 365.25,
          event = ifelse(censor_status == "Subject has had one or more events", 1, 0)
        ) %>%
        filter(!is.na(time_years), !is.na(event), time_years >= 0)
      
      km_fit_efs <<- survfit(Surv(time_years, event) ~ 1, data = efs_data)
      
      output$eventFreeSurvivalPlot <- renderPlot({
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
      })
      
      updateTabsetPanel(session, "tabs", selected = "Event-Free Survival")
      
    }, error = function(e) {
      output$errorMsg <- renderText({ paste("ERROR:", conditionMessage(e)) })
    })
  })
  
}

shinyApp(ui, server)