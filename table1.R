
tryCatch({
  required_pkgs <- c("shiny", "dplyr", "purrr", "tibble", 
                     "writexl", "sparklyr", "DT", "lubridate")
  
  install.packages(setdiff(required_pkgs, rownames(installed.packages())))
  
  invisible(lapply(required_pkgs, library, character.only = TRUE))
}, error = function(e) {
  message("An error occurred: ", e$message)
})


sc <- spark_connect(
  master = "local",
  spark_home = "/opt/spark-3.5.0",
  packages = c("org.apache.spark:spark-avro_2.12:3.5.0")
)

avro_file_path <- "/home/rstudio/export_2025-05-28T14_23_03.avro"
spark_df <- spark_read_avro(sc, name = "avro_data", path = avro_file_path)

entity_types <- spark_df %>%
  distinct(name) %>%
  collect() %>%
  pull(name)

dfs <- map(entity_types, ~ spark_df %>% filter(name == .x)) %>%
  set_names(entity_types)

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
        col <- gsub("-_,", "-", col)
        col <- gsub("- ", "-", col)
        col <- gsub("28", "(", col)
        col <- gsub("2e", ".", col)
        col <- gsub("29", ")", col)
        col <- gsub("/", "", col)
        col <- trimws(col)
      }
      col
    })
    
    final_df[is.na(final_df)] <- "None"
    final_df
  }, error = function(e) {
    message("Error flattening entity: ", e$message)
    tibble()
  })
}

aggregate_function <- function(df, selected_methods, date_col, relation) {
  if (length(relation) < 1) stop("relation must contain at least an id column")
  id_col <- relation[[1]]
  name_col <- if (length(relation) >= 2) relation[[2]] else NULL
  
  if (!id_col %in% names(df)) stop(paste0("ID column '", id_col, "' not found in data"))
  if (!is.null(name_col) && !name_col %in% names(df)) {
    warning(paste0("Name column '", name_col, "' not found; aggregating by id only"))
    name_col <- NULL
  }
  if (!date_col %in% names(df)) stop(paste0("Date column '", date_col, "' not found in data"))
  
  get_mode <- function(x) {
    x2 <- na.omit(x)
    if (length(x2) == 0) return(NA)
    ux <- unique(x2)
    ux[which.max(tabulate(match(x2, ux)))]
  }
  get_most_recent <- function(x, date_v) {
    if (all(is.na(date_v))) return(NA)
    idx <- which.max(date_v)
    if (length(idx) == 0) return(NA)
    x[idx]
  }
  get_most_distant <- function(x, date_v) {
    if (all(is.na(date_v))) return(NA)
    idx <- which.min(date_v)
    if (length(idx) == 0) return(NA)
    x[idx]
  }
  
  df[[date_col]] <- tryCatch({
    if (inherits(df[[date_col]], "POSIXt")) {
      df[[date_col]]
    } else {
      parsed <- suppressWarnings(as.POSIXct(df[[date_col]], tz = "UTC"))
      if (all(is.na(parsed))) {
        parsed2 <- suppressWarnings(lubridate::ymd_hms(df[[date_col]], quiet = TRUE, tz = "UTC"))
        if (all(is.na(parsed2))) {
          parsed3 <- suppressWarnings(as.POSIXct(as.numeric(df[[date_col]]), origin = "1970-01-01", tz = "UTC"))
          parsed3
        } else parsed2
      } else parsed
    }
  }, error = function(e) stop("Failed to parse date column: ", e$message))
  
  available_methods <- selected_methods[names(selected_methods) %in% names(df)]
  exclude_cols <- c(id_col, name_col, date_col) %>% unique() %>% na.omit()
  if (length(available_methods) > 0) {
    available_methods <- available_methods[!names(available_methods) %in% exclude_cols]
  }
  
  df <- df[order(df[[id_col]], df[[date_col]]), , drop = FALSE]
  
  group_cols <- c(id_col)
  if (!is.null(name_col)) group_cols <- c(group_cols, name_col)
  
  if (length(available_methods) == 0) {
    res <- df %>%
      dplyr::select(all_of(group_cols)) %>%
      distinct()
    return(res)
  }
  
  grouped <- df %>% dplyr::group_by(across(all_of(group_cols)))
  keys <- grouped %>% dplyr::group_keys()
  parts <- grouped %>% dplyr::group_split(.keep = TRUE)
  
  rows_out <- vector("list", length(parts))
  for (i in seq_along(parts)) {
    gdf <- parts[[i]]
    key_vals <- as.list(keys[i, , drop = FALSE])
    computed <- list()
    for (colname in names(available_methods)) {
      method <- available_methods[[colname]]
      vec <- gdf[[colname]]
      if (is.list(vec) && !is.atomic(vec)) {
        vec <- vapply(vec, function(x) if (length(x) == 0) NA else paste(x, collapse = " ; "), FUN.VALUE = character(1), USE.NAMES = FALSE)
      }
      if (all(is.na(vec))) {
        val <- NA
      } else {
        is_num <- is.numeric(vec)
        if (is_num && method == "mean") {
          val <- mean(vec, na.rm = TRUE)
        } else if (method %in% c("mode", "most_frequent")) {
          val <- get_mode(vec)
        } else if (method == "most_recent") {
          val <- get_most_recent(vec, gdf[[date_col]])
        } else if (method == "most_distant") {
          val <- get_most_distant(vec, gdf[[date_col]])
        } else if (is_num && method == "most_distant") {
          val <- get_most_distant(vec, gdf[[date_col]])
        } else if (is_num && method == "most_recent") {
          val <- get_most_recent(vec, gdf[[date_col]])
        } else {
          # fallback: NA (unknown method)
          val <- NA
        }
        
      }
      
      #new_colname <- paste0(colname, " (", method, ")")
      # computed[[new_colname]] <- if (length(val) == 0) NA else val
      
      computed[[colname]] <- if (length(val) == 0) NA else val
    }
    rows_out[[i]] <- as_tibble(c(key_vals, computed))
  }
  
  result <- dplyr::bind_rows(rows_out)
  result <- result %>% dplyr::relocate(all_of(group_cols))
  return(result)
}

ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      .col-row { display:flex; align-items:center; gap:12px; margin-bottom:6px; }
      .col-row .shiny-input-container { margin-bottom: 0 !important; }
      .entity-card { border:1px solid #e5e7eb; border-radius:12px; padding:12px; margin-bottom:16px; }
      .entity-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:8px; }
      .pill { font-size:12px; padding:2px 8px; border-radius:9999px; background:#f3f4f6; }
    "))
  ),
  titlePanel("Entity Flattening + Aggregation Tool"),
  sidebarLayout(
    sidebarPanel(
      checkboxGroupInput("selected_entities", "Select Entities to Flatten:", choices = entity_types),
      actionButton("flatten_btn", "Flatten Selected"),
      hr(),
      h4("Progress"),
      verbatimTextOutput("status")
    ),
    mainPanel(
      tabsetPanel(id = "tabs",
                  tabPanel("Results",
                           h4("Aggregation Configurations"),
                           uiOutput("summary_config_ui")
                  ),
                  tabPanel("Aggregated Data",
                           hr(),
                           h3("Show data"),
                           actionButton("show_data", "Show"),
                           DTOutput("aggregated_summary"),
                           hr(),
                           h3("Merged Data"),
                           DTOutput("merged_data"),
                           hr(),
                           uiOutput("col_selector"),
                           DTOutput("split_table")   
                  )
      )
    )
  )
)

server <- function(input, output, session) {
  progress_msgs <- reactiveVal("")
  flattened_dfs <- reactiveValues()
  
  observeEvent(input$flatten_btn, {
    req(input$selected_entities)
    msgs <- c()
    
    for (entity in input$selected_entities) {
      flat_df <- flatten_entity_df(dfs[[entity]])
      writexl::write_xlsx(flat_df, paste0(entity, "_flattened.xlsx"))
      flattened_dfs[[entity]] <- flat_df
      msgs <- c(msgs, paste0("Flattened ", nrow(flat_df), " records for ", entity))
      progress_msgs(paste(msgs, collapse = "\n"))
    }
  })
  
  output$status <- renderText(progress_msgs())
  
  output$summary_config_ui <- renderUI({
    entities <- names(reactiveValuesToList(flattened_dfs))
    if (length(entities) == 0) {
      return(tags$em("Flatten some entities first to configure settings."))
    }
    
    lapply(entities, function(entity) {
      df <- flattened_dfs[[entity]]
      if (is.null(df) || nrow(df) == 0) return(NULL)
      
      numeric_cols <- names(df)[sapply(df, is.numeric)]
      categorical_cols <- names(df)[sapply(df, function(x) is.factor(x) || is.character(x))]
      
      date_guess <- names(df)[grepl("date|time|dt|timestamp", names(df), ignore.case = TRUE)]
      date_choices <- names(df)
      if (length(date_guess) == 0) date_guess <- date_choices[1]
      
      col_row <- function(col, is_numeric) {
        div(class = "col-row",
            checkboxInput(inputId = paste0("col_", entity, "_", col), label = col, value = FALSE, width = "350px"),
            if (is_numeric) {
              selectInput(
                inputId = paste0("method_", entity, "_", col),
                label = NULL, width = "400px",
                choices = c(
                  "Most recent observation"                 = "most_recent",
                  "Most temporally distant observation"     = "most_distant",
                  "Most frequently occurring value (mode)"  = "mode",
                  "Arithmetic mean"                         = "mean"
                ),
                selected = "most_recent"
              )
            } else {
              selectInput(
                inputId = paste0("method_", entity, "_", col),
                label = NULL, width = "400px",
                choices = c(
                  "Most recent observation"                 = "most_recent",
                  "Most temporally distant observation"     = "most_distant",
                  "Most frequently occurring value"         = "mode"
                ),
                selected = "most_recent"
              )
            }
        )
      }
      
      div(class = "entity-card",
          div(class = "entity-header",
              checkboxInput(paste0("include_", entity), label = strong(entity), value = TRUE),
              span(class = "pill", paste0(nrow(df), " rows • ", ncol(df), " cols"))
          ),
          div(
            strong("Date column for temporal methods"),
            selectInput(paste0("date_", entity), NULL, choices = date_choices, selected = date_guess, width = "400px")
          ),
          if (length(numeric_cols)) {
            tagList(
              tags$hr(),
              strong("Numeric columns"),
              do.call(tagList, lapply(numeric_cols, function(cn) col_row(cn, TRUE)))
            )
          },
          if (length(categorical_cols)) {
            tagList(
              tags$hr(),
              strong("Categorical columns"),
              do.call(tagList, lapply(categorical_cols, function(cn) col_row(cn, FALSE)))
            )
          },
          actionButton(paste0("run_aggregate_", entity), "Run Aggregation"),
          verbatimTextOutput(paste0("selection_output_", entity)),
          DTOutput(paste0("agg_table_", entity))  # Only aggregated results
      )
    })
  })
  aggregated_dfs <- reactiveValues()
  
  observe({
    entities <- names(reactiveValuesToList(flattened_dfs))
    for (entity in entities) {
      local({
        e <- entity
        observeEvent(input[[paste0("run_aggregate_", e)]], {
          df <- flattened_dfs[[e]]
          if (is.null(df)) return(NULL)
          
          checked_cols <- names(df)[vapply(names(df), function(col) {
            isTRUE(input[[paste0("col_", e, "_", col)]])
          }, logical(1))]
          
          if (length(checked_cols) == 0) {
            output[[paste0("selection_output_", e)]] <- renderText("No columns selected.")
            return(NULL)
          }
          
          methods <- purrr::map_chr(checked_cols, function(col) {
            val <- input[[paste0("method_", e, "_", col)]]
            if (is.null(val)) "most_recent" else val
          }) %>% set_names(checked_cols)
          
          methods <- methods[names(methods) %in% names(df)]
          if (length(methods) == 0) {
            output[[paste0("selection_output_", e)]] <- renderText("Selected columns not found in data.")
            return(NULL)
          }
          
          date_col <- input[[paste0("date_", e)]]
          
          id_candidates <- c("dst_id", "id", "patient_id", "subject_id")
          name_candidates <- c("dst_name", "name", "dst_label", "label")
          id_col <- intersect(id_candidates, names(df))
          name_col <- intersect(name_candidates, names(df))
          id_col <- if (length(id_col) >= 1) id_col[[1]] else NULL
          name_col <- if (length(name_col) >= 1) name_col[[1]] else NULL
          
          if (is.null(id_col)) {
            output[[paste0("selection_output_", e)]] <- renderText("No ID-like column found (dst_id/id/patient_id...). Cannot aggregate.")
            return(NULL)
          }
          
          relation <- if (!is.null(name_col)) list(id_col, name_col) else list(id_col)
          
          agg_df <- tryCatch({
            aggregate_function(df, methods, date_col, relation)
          }, error = function(err) {
            structure(list(error = TRUE, message = err$message), class = "agg_error")
          })
          
          if (inherits(agg_df, "agg_error")) {
            output[[paste0("selection_output_", e)]] <- renderText(paste0("Aggregation error: ", agg_df$message))
            return(NULL)
          }
          
          aggregated_dfs[[e]] <- list(
            data = agg_df,
            methods = methods
          )
          sel_text <- paste(
            "Before aggregation:", nrow(df), "records",
            "\nAfter aggregation:", nrow(agg_df), "records",
            "\n\nDate column:", date_col,
            "\nGrouping by:", paste(relation, collapse = " , "),
            "\nSelected columns and methods:",
            paste(paste(names(methods), methods, sep = " -> "), collapse = "\n")
          )
          output[[paste0("selection_output_", e)]] <- renderText(sel_text)
          
          # Display aggregated result
          output[[paste0("agg_table_", e)]] <- renderDT(
            agg_df,
            options = list(
              pageLength = 10,
              lengthMenu = c(5, 10, 25, 50, 100),
              scrollX = TRUE
            ),
            rownames = TRUE,
            filter = "top"
          )
          
        }, ignoreInit = TRUE)
      })
    }
  })
  observeEvent(input$show_data, {
    all_entities <- names(reactiveValuesToList(aggregated_dfs))
    
    if (length(all_entities) == 0) {
      output$aggregated_summary <- renderDT({
        data.frame(
          Entity = character(0),
          Methods = character(0),
          Relation = character(0),
          DateColumn = character(0)
        )
      })
      return(NULL)
    }
    
    summary_list <- lapply(all_entities, function(e) {
      agg_entry <- aggregated_dfs[[e]]
      if (is.null(agg_entry)) return(NULL)
      
      methods <- if (!is.null(agg_entry$methods)) {
        paste(paste(names(agg_entry$methods), agg_entry$methods, sep = " -> "), collapse = "; ")
      } else {
        NA
      }
      
      relation <- if (!is.null(agg_entry$relation)) {
        paste(agg_entry$relation, collapse = ", ")
      } else {
        NA
      }
      
      date_col <- if (!is.null(agg_entry$date_col)) agg_entry$date_col else NA
      
      data.frame(
        Entity = e,
        Methods = methods,
        Relation = relation,
        DateColumn = date_col,
        stringsAsFactors = FALSE
      )
    })
    
    summary_df <- do.call(rbind, summary_list)
    
    output$aggregated_summary <- renderDT(
      summary_df,
      options = list(
        pageLength = 10,
        autoWidth = TRUE,
        scrollX = TRUE
      ),
      rownames = FALSE
    )
    
    if ("subject" %in% all_entities) {
      subject_df <- aggregated_dfs[["subject"]]$data
      if (!is.null(subject_df) && "id" %in% names(subject_df)) {
        merged_df <- subject_df
        for (e in setdiff(all_entities, "subject")) {
          df <- aggregated_dfs[[e]]$data
          if (!is.null(df) && "dst_id" %in% names(df)) {
            merged_df <- dplyr::inner_join(
              merged_df, df,
              by = c("id" = "dst_id")
            )
          }
        }
        
        for (e in names(aggregated_dfs)) {
          methods <- aggregated_dfs[[e]]$methods
          if (!is.null(methods)) {
            for (col in names(methods)) {
              if (col %in% names(merged_df) && !(col %in% c("id", "dst_id", "name"))) {
                new_name <- paste0(col, " (", methods[[col]], ")")
                names(merged_df)[names(merged_df) == col] <- new_name
              }
            }
          }
        }
        
        output$merged_data <- DT::renderDT(
          merged_df,
          options = list(
            pageLength = 10,
            scrollX = TRUE
          ),
          rownames = FALSE,
          filter = "top"
        )
        
        output$col_selector <- renderUI({
          req(merged_df) 
          selectInput(
            inputId = "split_col",
            label = "Select a column to group data by:",
            choices = names(merged_df),
            selected = names(merged_df)[1]
          )
        })
        
        output$split_table <- DT::renderDT({
          req(input$split_col, merged_df)
          
          col <- input$split_col
          
          split_df <- merged_df %>%
            dplyr::group_by(.data[[col]]) %>%
            dplyr::summarise(dplyr::across(
              .cols = where(is.numeric),
              .fns = list(
                count = ~sum(!is.na(.)),
                percentage = ~round(sum(!is.na(.)) / nrow(merged_df) * 100, 2)
              ),
              .names = "{.col}_{.fn}"
            ))
          datatable(
            split_df,
            options = list(
              pageLength = 10,
              scrollX = TRUE
            ),
            rownames = FALSE
          )
        })
      }
    }
  })  
}

shinyApp(ui, server)