library(shiny)
library(reticulate)
library(config)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)
library(dygraphs)
library(dutils)
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


# UI ----------------------------------------------------------------------
ui <- fluidPage(
    titlePanel("Bomba de calor lab eXiT"),
    sidebarLayout(
        sidebarPanel = sidebarPanel(
            dateInput("date", "Dia a visualitzar:", 
                      value = get_current_date(),
                      min = dmy(01122020), max = get_current_date()),
            HTML(
                "
                <p>
                Explicació de les variables de temperatura:
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
                    <li><b>Temperature_S1</b>: Temperatura de la sonda de 50m dels pous</li>
                </ul>
                "
            ),
            fluidRow(
                align = 'center',
                downloadButton("download", "Descarrega't les temperatures (Excel)")
            ),
            hr(),
            fluidRow(
                HTML(
                    "
                    <p>
                    Explicació de les variables d'informació:
                    </p>
                    <ul>
                        <li><b>Mode d'operació</b>: Buffer (1), Buffer+Cooling (2), 1 Zone (3), 1 Zone - 2 Systems (4), 1 Zone - Multiemitter (5), 2 Zones (6), 2 Zones - 2 Systems (7)</li>
                        <li><b>Mode de configuració:</b>: Hivern (0), Estiu (1), Automàtic (2)</li>
                        <li><b>Alarma</b>: Amb alarmes FALSE, sense alarmes TRUE</li>
                        <li><b>Estat ON/OFF</b>: Bomba ON (0), bomba OFF (1)</li>
                        <li><b>COP</b>: Coefficient of Performance. Efficiència de la bomba de calor.</li>
                        <li><b>EER</b>: Energy Efficiency Ratio. Efficiència de la bomba de calor <b> només per refrigeració </b>.</li>
                    </ul>
                    "
                )
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
                    uiOutput('controls')
                ),
                tabPanel(
                    "Info",
                    tabsetPanel(
                        type = "pills",
                        tabPanel(
                            "Configuració",
                            dataTableOutput("table_config")
                        ),
                        tabPanel(
                            "Funcionament",
                            dataTableOutput("table_funcionament")
                        ),
                        tabPanel(
                            "Rendiment",
                            dygraphOutput("plot_cop")
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
    data_hp <- reactive({
        response <- table$query(
            KeyConditionExpression = dbKey("day")$eq(as.character(input$date))
        )
        
        data <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
            mutate(datetime = with_tz(force_tz(as_datetime(paste(day, time)), tzone = 'CEST'), tzone = 'Europe/Madrid')) %>% 
            select(datetime, everything(), -day, -time)
        data
    })
    
    temperatures <- reactive({
        select(data_hp(), any_of(c('datetime', 'Tm,i', 'Td', 'Te', 'Tm,r', 'Tp,impulsion', 'Tp,return', 'Temperature_S1')))
    })
    
    info <- reactive({
        select(data_hp(), any_of(c('datetime', 'E_power', 'COP', 'EER', 'CONFIG', 'Operation_mode', 'Td,c_winter', 'Td,c_summer', 'Status message', 'Alarm', 'State ON/OFF')))
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
            dyplot(title = "<h4><center>Gràfic de temperatures</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_pous <- renderDygraph({
        total_data() %>% 
            select(any_of(c("datetime", grep('Tp', names(.)), 'Te', 'Temperature_S1'))) %>% 
            dyplot(title = "<h4><center>Gràfic de temperatures dels pous</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_diposit <- renderDygraph({
        total_data() %>% 
            select(any_of(c("datetime", grep('Tm', names(.)), "Td"))) %>%
            dyplot(title = "<h4><center>Gràfic de temperatures del dipòsit</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_lab <- renderDygraph({
        total_data() %>% 
            select(any_of(c("datetime", grep('Temp_4', names(.)), "Tint", "Te"))) %>% 
            dyplot(title = "<h4><center>Gràfic de temperatures del laboratori</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$download  <- downloadHandler(
        filename = function() {paste0("hp_lab_", Sys.Date(), ".xlsx")},
        content = function(file) {
            writexl::write_xlsx(total_data(), path = file)
        }
    )
    

    # Controls ---------------------------------------------------------------
    
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
            control_inputs_names(),
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
            control_inputs_names(), 
            ~ update_control_value(table_control, .x, input[[.x]])
        )
    })
    
    control_inputs_names <- reactive({
        setpoints_inputs_list <- pmap(hp_control_updated(), ~ paste0('setpoint', '_', ..1, '_', ..2))
        speeds_inputs_list <- pmap(hp_control_updated(), ~ paste0('speed', '_', ..1, '_', ..2))
        c(setpoints_inputs_list, speeds_inputs_list)
    })
    
    # Controls
    output$controls <- renderUI({
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
                tableInput(hp_control_updated(), "weekday")
            ),
            column(
                5,
                tableInput(hp_control_updated(), "weekend")
            )
        )
    })
    

# Informació --------------------------------------------------------------

    output$table_config <- renderDataTable({
        config_tbl <- info()[, c('datetime', 'CONFIG', 'Td,c_winter', 'Td,c_summer', 'Operation_mode')]
        colnames(config_tbl) <- c('Dia i hora', 'Mode de configuració', 'Consigna dipòsit hivern', 'Consigna dipòsit estiu', "Mode d'operació")
        return( config_tbl )
    })
    
    output$table_funcionament <- renderDataTable({
        funcionament_tbl <- info()[, c('datetime', 'Alarm', 'State ON/OFF')]
        colnames(funcionament_tbl) <- c('Dia i hora', 'Alarma', 'Estat ON/OFF')
        return( funcionament_tbl )
    })
    
    output$plot_cop <- renderDygraph({
        info() %>% 
            select('datetime', 'E_power', 'COP', 'EER') %>% 
            dyplot(ylab = '<b>Temperatura (ºC)</b>', strokeWidth = 2)
            
    })

}

# Run the application 
shinyApp(ui = ui, server = server)
