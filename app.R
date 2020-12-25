library(shiny)
library(reticulate)
library(config)
library(dplyr)
library(tidyr)
library(lubridate)
library(dygraphs)
library(dutils)
library(purrr)
source('utils.R')

config <- config::get(file = 'config.yml')

# Python
reticulate::use_python(config$python_path, required = T) # Restart R session to change the python env
# reticulate::source_python("support/dynamodb_utils.py")
boto3 <- import("boto3")
dbKey <- import("boto3.dynamodb")$conditions$Key
dbAttr <- import("boto3.dynamodb")$conditions$Attr

# Database import --------------------------------------------------
dynamodb <- boto3$resource('dynamodb',
                           aws_access_key_id = config$dynamodb$access_key_id,
                           aws_secret_access_key = config$dynamodb$secret_access_key,
                           region_name = config$dynamodb$region_name)

table <- dynamodb$Table(config$dynamodb$table_hp)
table_libelium <- dynamodb$Table(config$dynamodb$table_meshlium)
table_control <- dynamodb$Table(config$dynamodb$table_control)

# Get initial control table
response <- table_control$scan()
hp_control <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>%
    rename(day_type = `day-type`) %>%
    select(day_type, hour, setpoint, speed)
setpoints_inputs_list <- pmap(hp_control, ~ paste0('setpoint', '_', ..1, '_', ..2))
speeds_inputs_list <- pmap(hp_control, ~ paste0('speed', '_', ..1, '_', ..2))
control_inputs_names <- c(setpoints_inputs_list, speeds_inputs_list)


# UI ----------------------------------------------------------------------
ui <- fluidPage(
    titlePanel("Bomba de calor lab eXiT"),
    sidebarLayout(
        sidebarPanel = sidebarPanel(
            dateInput("date", "Dia a visualitzar:", 
                      value = today(),
                      min = dmy(01122020), max = today()),
            HTML(
                "
                <p>
                Explicació de les variables:
                </p>
                <ul>
                    <li><b>Tm,i</b>: Temperatura màquina - dipòsit</li>
                    <li><b>Tm,r</b>: Temperature dipòsit - màquina</li>
                    <li><b>Tp,i</b>: Temperatura màquina - pou</li>
                    <li><b>Tp,r</b>: Temperatura pou - màquina</li>
                    <li><b>Td</b>: Temperatura dipòsit</li>
                    <li><b>Te</b>: Temperatura exterior</li>
                    <li><b>Temp_*</b>: Temperatura dels sensors del laboratori</li>
                    <li><b>Tint</b>: Temperatura mitjana dels sensors del laboratori</li>
                </ul>
                "
            ),
            hr(),
            fluidRow(
                downloadButton("download", "Descarrega't les dades (Excel)")
            )
        ),
        mainPanel(
            tabsetPanel(
                type = "tabs",
                tabPanel(
                    "Gràfics",
                    tabsetPanel(
                        type = "pills",
                        tabPanel(
                            "Tot",
                            dygraphOutput("plot")
                        ),
                        tabPanel(
                            "Pous",
                            dygraphOutput("plot_pous")
                        ),
                        tabPanel(
                            "Dipòsit",
                            dygraphOutput("plot_diposit")
                        ),
                        tabPanel(
                            "Lab",
                            dygraphOutput("plot_lab")
                        )
                    )
                    
                ),
                tabPanel(
                    "Controls",
                    br(),
                    fluidRow(
                        column(
                            2,
                            actionButton("pull_config", "Pull", icon = icon("cloud-download-alt")),
                            actionButton("push_config", "Push", icon = icon("cloud-upload-alt"))
                        ),
                        column(
                            5,
                            h4("Dilluns - Divendres")
                        ),
                        column(
                            5,
                            h4("Dissabte - Diumenge")
                        )
                    ),
                    fluidRow(
                        column(
                            2,
                            h5("Hora"),
                            map(
                                as.list(0:23),
                                ~ numericInput(paste0('hour', '_', .x), NULL, .x)
                            )
                        ),
                        column(
                            5,
                            tableInput(hp_control, "weekday")
                        ),
                        column(
                            5,
                            tableInput(hp_control, "weekend")
                        )
                    )
                    
                )
            )
        )
    )
)


# Server ------------------------------------------------------------------
server <- function(input, output, session) {
    

    # Gràfic ------------------------------------------------------------------
    temperatures <- reactive({
        response <- table$query(
            KeyConditionExpression = dbKey("day")$eq(as.character(input$date))
        )
        
        data <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
            mutate(datetime = with_tz(force_tz(as_datetime(paste(day, time)), tzone = 'CEST'), tzone = 'Europe/Madrid')) %>% 
            select(datetime, everything(), -day, -time)
        
        data[, c(1, 4, 11, 12, 14, 16, 17)]
    })
    
    tint <- reactive({
        response <- table_libelium$scan(
            FilterExpression = dbAttr("ts")$between(as.integer(as_datetime(input$date-hours(30))), as.integer(as_datetime(input$date+days(1))))
        )
        
        data <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
            mutate(datetime = with_tz(as_datetime(ts), tzone = 'Europe/Madrid')) %>% 
            select(datetime, Tint, everything()) %>% 
            arrange(datetime)
    
        data[, c(1, 2, grep('Temp_4', names(data)))]
    })
    
    total_data <- reactive({
        temperatures() %>% 
            left_join(tint(), by = 'datetime') %>% 
            fill(colnames(tint())[c(2, grep('Temp_4', names(tint())))], .direction = 'downup')
    }) 
    
    output$plot <- renderDygraph({
        total_data() %>% 
            df_to_ts() %>% 
            dygraph("<h4><center>Gràfic de temperatures</center></h4>", ylab = "Temperatura (ºC)") %>% 
            format_dygraph(strokeWidth = 2)
    })
    
    output$plot_pous <- renderDygraph({
        total_data() %>% 
            select(c("datetime", grep('Tp', names(.)), Te)) %>% 
            df_to_ts() %>% 
            dygraph("<h4><center>Gràfic de temperatures dels pous</center></h4>", ylab = "Temperatura (ºC)") %>% 
            format_dygraph(strokeWidth = 2)
    })
    
    output$plot_diposit <- renderDygraph({
        total_data() %>% 
            select(c("datetime", grep('Tm', names(.)), "Td")) %>% 
            df_to_ts() %>% 
            dygraph("<h4><center>Gràfic de temperatures del dipòsit</center></h4>", ylab = "Temperatura (ºC)") %>% 
            format_dygraph(strokeWidth = 2)
    })
    
    output$plot_lab <- renderDygraph({
        total_data() %>% 
            select(c("datetime", grep('Temp_4', names(.)), "Tint", "Te")) %>% 
            df_to_ts() %>% 
            dygraph("<h4><center>Gràfic de temperatures del laboratori</center></h4>", ylab = "Temperatura (ºC)") %>% 
            format_dygraph(strokeWidth = 2)
    })
    
    output$download  <- downloadHandler(
        filename = function() {paste0("hp_lab_", today(), ".xlsx")},
        content = function(file) {
            writexl::write_xlsx(total_data(), path = file)
        }
    )
    

    # Consignes ---------------------------------------------------------------
    
    # Pull control table when Pull
    hp_control_updated <- eventReactive(input$pull_config, ignoreNULL = FALSE, {
        response <- table_control$scan()
        map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
            rename(day_type = `day-type`) %>% 
            select(day_type, hour, setpoint, speed)
    })
    
    # Update UI values
    observeEvent(input$pull_config, {
        message("Pulling from DynamoDB")
        walk(
            control_inputs_names,
            ~ updateNumericInput(
                session, .x, 
                value = get_control_value(hp_control_updated(), .x)
            )
        )
    })
    
    # Update DynamoDB when Push
    observeEvent(input$push_config, {
        message("Pushing to DynamoDB")
        walk(
            control_inputs_names, 
            ~ update_control_value(table_control, .x, input[[.x]])
        )
    })

}

# Run the application 
shinyApp(ui = ui, server = server)
