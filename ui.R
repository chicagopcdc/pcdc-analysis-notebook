# ui.R
ui <- fluidPage(
  theme = shinytheme("flatly"),
  useShinyjs(),
  
  tags$head(
    tags$style(HTML("
      .brand { font-weight:800; letter-spacing:.5px; font-size:20px; }
      .step-badge { display:inline-flex; align-items:center; gap:8px; padding:8px 12px; border-radius:999px; background:#f1f5f9; margin-right:8px; }
      .step-badge.done { background:#d1fae5; }
      .step-badge.locked { background:#eee; opacity:.7; }
      .muted { color:#64748b; font-size:13px; }
      .sticky-sidebar { position:sticky; top:12px; }
      .footer { font-size:12px; color:#94a3b8; margin-top:12px; }
      .card h4 { font-weight:700; margin-bottom:10px; }

      /* Attractive Upload Box */
      .upload-box {
        border: 2px dashed #4CAF50;
        border-radius: 12px;
        padding: 24px;
        background: #f9fff9;
        text-align: center;
        font-weight: 600;
        font-size: 14px;
        color: #2e7d32;
        transition: background 0.3s ease;
        cursor: pointer;
        margin-top: 12px;
      }
      .upload-box:hover {
        background: #e8f5e9;
      }

      /* Buttons Consistency */
      .btn { font-weight:600; }
      .btn-success { background-color:#16a34a; border:none; }
      .btn-info { background-color:#0284c7; border:none; }
      .btn-outline-secondary { font-weight:500; }
    "))
  ),
  
  titlePanel(
    div(
      span("Avro Insights", class = "brand"),
      br(),
      span(
        "Upload → Select → Explore → Analyze → Summarize",
        class = "muted"
      )
    )
  ),
  
  fluidRow(
    # Sidebar column
    column(
      width = 3,
      div(
        class = "sticky-sidebar",
        wellPanel(
          h4("1) Upload AVRO", style = "margin-top:0;"),
          fileInput("avroFile", "Choose AVRO File:", accept = ".avro"),
          textOutput("filePath"),
          hr(),
          
          h4("2) Choose Entities (optional)"),
          p(
            class = "muted",
            "We always include 'subject', 'person', 'timing', and 'survival_characteristic'. Select additional entities to join if needed."
          ),
          checkboxInput("select_all_entities", "Select All", FALSE),
          uiOutput("entity_picker"),
          hr(),
          
          h4("3) Prepare Data"),
          actionButton("prep_btn", "Prepare (Flatten Selected Only)", class = "btn btn-primary", width = "100%"),
          br(), br(),
          actionButton("reset_btn", "Start Over", class = "btn btn-outline-secondary", width = "100%"),
          hr(),
          
          h5("Progress"),
          htmlOutput("progress_steps"),
          div(class = "footer", "D4CG · AVRO File Analysis Tool")
        )
      )
    ),
    
    # Main content column
    column(
      width = 9,
      tabsetPanel(
        id = "mainTabs",
        
        # Distribution Tab
        tabPanel(
          title = "Distribution",
          value = "Distribution",
          br(),
          wellPanel(
            fluidRow(
              column(6, selectInput("dist_col", "Select Column", choices = character(0))),
              column(
                6,
                div(
                  style = "margin-top:26px; text-align:right;",
                  actionButton("dist_submit", "Generate Distribution", class = "btn btn-info"),
                  actionButton("go_surv_from_dist", "Next: Overall Survival →", class = "btn btn-success", style = "margin-left:6px;")
                )
              )
            )
          ),
          plotOutput("percentagePlot", height = "600px"),
          br(),
          fluidRow(
            column(6, downloadButton("download_dist_xlsx", "Download Percentages (.xlsx)", class = "btn btn-light")),
            column(6, downloadButton("download_dist_png",  "Download Plot (.png)", class = "btn btn-light"))
          ),
          br(),
          textOutput("dist_msg")
        ),
        
        # Survival Tab
        tabPanel(
          title = "Survival",
          value = "Survival",
          br(),
          wellPanel(
            h4("Overall Survival"),
            p(class = "muted", "Uses pre-flattened timing + survival_characteristic."),
            actionButton("overallSurvivalBtn", "Show Overall Survival Curve", class = "btn btn-info"),
            plotOutput("overallSurvivalPlot", height = "700px"),
            downloadButton("download_os_png", "Download OS Plot (.png)", class = "btn btn-light"),
            br(), br(),
            actionButton("go_efs", "Next: Event-Free Survival →", class = "btn btn-success")
          ),
          wellPanel(
            h4("Event-Free Survival"),
            p(class = "muted", "Uses pre-flattened subject + timing (Initial Diagnosis)."),
            actionButton("eventFreeSurvivalBtn", "Show Event-Free Survival Curve", class = "btn btn-info"),
            plotOutput("eventFreeSurvivalPlot", height = "700px"),
            downloadButton("download_efs_png", "Download EFS Plot (.png)", class = "btn btn-light"),
            br(), br(),
            actionButton("go_table1", "Next: Summary Table →", class = "btn btn-success")
          )
        ),
        
        # Table 1 Tab
        tabPanel(
          title = "Summary Table",
          value = "Table1",
          br(),
          
          wellPanel(
            h4("Covariate Filters"),
            div(
              class = "upload-box",
              fileInput(
                "upload_excel",
                label = "📂 Upload Subject IDs (Excel/TXT/CSV)",
                accept = c(".xlsx", ".xls", ".txt", ".csv"),
                buttonLabel = "Browse...",
                placeholder = "No file selected"
              ),
              p(class = "muted",
                "➡ Upload TXT (IDs line by line) or CSV/Excel (with 'SubjectID' column) → defines Group 1; all others = Group 2."
              )
            ),
            br(), 
            tags$div(
              id = "filter_container",
              tags$div(
                id = "no_cards_msg", 
                class = "muted", 
                "No covariates added. Click 'Add Covariate' to begin."
              )
            ),br(),
            # Action buttons
            actionButton("add_covariate", "Add Covariate", class = "btn btn-success"),
            actionButton("apply_filters", "Generate Table 1", class = "btn btn-info", style = "margin-left:6px;"),
            actionButton("clear_cards", "Clear All", class = "btn btn-outline-secondary", style = "margin-left:6px;"),
            
          ),
          
          uiOutput("table1_ui"),
          br(),
          downloadButton("download_table1_xlsx", "Download Table 1 (.xlsx)", class = "btn btn-light")
        )
        
      ),
      br(),
      verbatimTextOutput("status"),
      verbatimTextOutput("errorMsg")
    )
  )
)