library(shiny)
library(config)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)
library(dygraphs)
library(dutils)
source('utils.R')
options(scipen=999)


config <- config::get(file = 'config.yml')


reticulate::use_python(config$python_path, required = T) # Restart R session to change the python env
boto3 <- reticulate::import("boto3")
# dbKey <- reticulate::import("boto3.dynamodb")$conditions$Key



# DynamoDB client --------------------------------------------------
# DynamoDB Lab
dynamodb <- get_dynamodb_py(config$dynamodb$access_key_id, config$dynamodb$secret_access_key, config$dynamodb$region_name)
table <- get_dynamo_table_py(dynamodb, config$dynamodb$table_hp)
table_libelium <- get_dynamo_table_py(dynamodb, config$dynamodb$table_meshlium)
table_control <- get_dynamo_table_py(dynamodb, config$dynamodb$table_control)

# DynamoDB Marc
dynamodb_marc <- get_dynamodb_py(config$dynamodb_marc$access_key_id, config$dynamodb_marc$secret_access_key, config$dynamodb_marc$region_name)
table_dht <- get_dynamo_table_py(dynamodb_marc, config$dynamodb_marc$table_dht)
table_power <- get_dynamo_table_py(dynamodb_marc, 'power-sensors')
# tpgx <- query_timeseries_data_table(table_power, "id", "9cce", "timestamp", Sys.Date() - days(1), Sys.Date(), time_interval_mins = 5, spread_column = 'data')

# IoT client --------------------------------------------------------------
iot <- boto3$client('iot-data',
                    aws_access_key_id = config$dynamodb$access_key_id,
                    aws_secret_access_key = config$dynamodb$secret_access_key,
                    region_name = config$dynamodb$region_name)


# UI ----------------------------------------------------------------------
ui <- fluidPage(
    titlePanel("Bomba de calor lab eXiT"),
    hr(),
    sidebarLayout(
        sidebarPanel = sidebarPanel(
            dateRangeInput(
                "dates", "Dia a visualitzar:", 
                start = Sys.Date(), end = Sys.Date(),
                min = dmy(01122020), max = Sys.Date(),
                weekstart = 1, language = "ca",
            ),
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
                    <li><b>DHT_sensor</b>: Temperatura del sensor DHT22 del laboratori</li>
                    <li><b>heat_index</b>: Índex de temperatura (sensació tèrmica per humitat)</li>
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
                        <li><b>Mode de configuració</b>: Buffer (1), Buffer+Cooling (2), 1 Zone (3), 1 Zone - 2 Systems (4), 1 Zone - Multiemitter (5), 2 Zones (6), 2 Zones - 2 Systems (7)</li>
                        <li><b>Mode d'operació</b>: Hivern (0), Estiu (1), Automàtic (2)</li>
                        <li><b>Alarma</b>: Amb alarmes FALSE, sense alarmes TRUE</li>
                        <li><b>Estat ON/OFF</b>: Bomba ON (0), bomba OFF (1)</li>
                        <li><b>Potència</b>: Potència elèctrica (W) consumida per la bomba de calor.</li>
                        <li><b>COP</b>: Coefficient of Performance. Efficiència de la bomba de calor.</li>
                        <li><b>EER</b>: Energy Efficiency Ratio. Efficiència de la bomba de calor <b> només per refrigeració </b>.</li>
                    </ul>
                    "
                )
            ),
            fluidRow(
                align = 'center',
                downloadButton("download_info", "Descarrega't la informació (Excel)")
            ),
            hr(),
            passwordInput('controls_password', "Contrassenya d'administrador:", width = '50%')
        ),
        mainPanel(
            tabsetPanel(
                id = "tabs",
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
                        ),
                        tabPanel(
                            "Consum",
                            dygraphOutput("plot_power")
                        )
                    )
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
                        )
                    )
                ),
                    tabPanel(
                        "Controls",
                        tabsetPanel(
                            type = "pills",
                            tabPanel(
                                "Fancoil",
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
                                uiOutput('controls_fancoil')
                            ),
                            tabPanel(
                                "Dipòsit",
                                uiOutput('controls_diposit')
                            )
                        )
                    )

            )
        )
    )
)


# Server ------------------------------------------------------------------
server <- function(input, output, session) {
    
    # Query data ------------------------------------------------------------------
    # Modbus data
    data_hp <- reactive({
        # data <- dynamodb_query_items(table, "day", as.character(seq.Date(input$dates[1], input$dates[2], by = 'day'))) 
        data <- query_table_py(table, "day", as.character(seq.Date(input$dates[1], input$dates[2], by = 'day'))) %>% 
            mutate(datetime = with_tz(force_tz(as_datetime(paste(day, time)), tzone = 'CEST'), tzone = 'Europe/Madrid')) %>% 
            select(datetime, everything(), -day, -time)
        data
    })
    
    # Libelium data
    tint <- reactive({
        data <- scan_table_py(table_libelium, "ts", as.integer(as_datetime(input$dates[1]-hours(10))), as.integer(as_datetime(input$dates[2]+hours(10)))) %>% 
            mutate(datetime = as_datetime(ts, tz = 'Europe/Madrid')) %>% 
            select(datetime, Tint, everything()) %>% 
            arrange(datetime)
        
        data[, c(1, 2, grep('Temp_4', names(data)))]
    })
    
    # DHT22 data
    dht_sensor <- reactive({
        query_timeseries_data_table_py(table_dht, "id", "4CFB", "timestamp", input$dates[1], input$dates[2]+days(1)) %>% 
            mutate(
                datetime = floor_date(as_datetime(timestamp/1000, tz = config$tzone), '10 minutes'),
                map_dfr(data, ~ .x)
            ) %>% 
            select(datetime, DHT_sensor = temperature, heat_index)
    })
    
    # Power sensor data
    power_sensor <- reactive({
        query_timeseries_data_table_py(table_power, "id", "E8EC4412CFA4", "timestamp", input$dates[1], input$dates[2]+days(1)) %>% 
            mutate(
                datetime = floor_date(as_datetime(timestamp/1000, tz = config$tzone), '5 minutes'),
                map_dfr(data, ~ .x)
            ) %>% 
            mutate(power_demand = current*240) %>% 
            select(datetime, power_demand) %>% 
            decrease_resolution(10, 'average')
    })
    

    # Group data --------------------------------------------------------------

    info <- reactive({
        select(data_hp(), any_of(c('datetime', 'COP', 'EER', 'CONFIG', 'Operation_mode', 'Td,c_winter', 'Td,c_summer', 'Status message', 'Alarm', 'State ON/OFF'))) %>% 
            left_join(power_sensor(), by = 'datetime') %>% 
            fill(power_demand, .direction = 'downup')
    })
    
    hp_temperatures <- reactive({
        select(data_hp(), any_of(c('datetime', 'Tm,i', 'Td', 'Te', 'Tm,r', 'Tp,impulsion', 'Tp,return', 'Temperature_S1')))
    })
    
    temperatures_data <- reactive({
        hp_temperatures() %>% 
            left_join(tint(), by = 'datetime') %>% 
            left_join(dht_sensor(), by = 'datetime') %>% 
            fill(c(colnames(tint())[-1], colnames(dht_sensor())[-1]), .direction = 'downup')
    }) 
    

    # Gràfics temperatures -------------------------------------------------------------------
    
    output$plot <- renderDygraph({
        temperatures_data() %>% 
            dyplot(title = "<h4><center>Gràfic de temperatures</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_pous <- renderDygraph({
        temperatures_data() %>% 
            select(datetime, starts_with("Tp"), Te, Temperature_S1) %>% 
            dyplot(title = "<h4><center>Gràfic de temperatures dels pous</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_diposit <- renderDygraph({
        temperatures_data() %>% 
            select(datetime, starts_with("Tm"), Td) %>%
            dyplot(title = "<h4><center>Gràfic de temperatures del dipòsit</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_lab <- renderDygraph({
        temperatures_data() %>% 
            select(datetime, starts_with("Temp_"), Tint, Te, DHT_sensor, heat_index) %>% 
            dyplot(title = "<h4><center>Gràfic de temperatures del laboratori</center></h4>", ylab = "Temperatura (ºC)", strokeWidth = 2)
    })
    
    output$plot_power <- renderDygraph({
        info() %>% 
            select(datetime, power_demand, COP, EER) %>% 
            dyplot(title = "<h4><center>Gràfic de potència i rendiment</center></h4>", strokeWidth = 2) %>% 
            dyAxis('y', 'Potència (W)') %>% 
            dyAxis('y2', 'kWh tèrmics / kWh elèctrics') %>% 
            dySeries('power_demand', 'Consum elèctric') %>% 
            dySeries('COP', axis = 'y2') %>% 
            dySeries('EER', axis = 'y2')
    })


    # Descàrrega de fitxers ---------------------------------------------------
    
    output$download  <- downloadHandler(
        filename = function() {paste0("hp_lab_temperatures_", Sys.Date(), ".xlsx")},
        content = function(file) {
            writexl::write_xlsx(temperatures_data(), path = file)
        }
    )
    
    output$download_info  <- downloadHandler(
        filename = function() {paste0("hp_lab_information_", Sys.Date(), ".xlsx")},
        content = function(file) {
            writexl::write_xlsx(info(), path = file)
        }
    )
    

    # Controls ---------------------------------------------------------------
    
    observe({
        if (is.null(input$controls_password) || input$controls_password != config$controls_pwd) {
            hideTab(inputId = "tabs", target = "Controls")
        } else {
            showTab(inputId = "tabs", target = "Controls")
        }
    })

    
    # Pull control table when Pull
    hp_control_updated <- eventReactive(input$pull_config, ignoreNULL = FALSE, {
        response <- table_control$scan()
        map_dfr(response$Items, ~ as_tibble(parse_python_object(.x))) %>% 
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
    
    # Controls fancoil
    output$controls_fancoil <- renderUI({
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
    
    
    # Controls dipòsit
    output$controls_diposit <- renderUI({
        Td <- as.list(info()[nrow(info()), c('Td,c_winter', 'Td,c_summer')])
        tagList(
            br(),
            fluidRow(
                column(
                    3,
                    numericInput('Td_winter', 'Consigna mode hivern (ºC)', Td[['Td,c_winter']], 25, 55, 0.5)
                ),
                column(
                    3,
                    numericInput('Td_summer', 'Consigna mode estiu (ºC)', Td[['Td,c_summer']], 8, 25, 0.5)
                )
            ),
            fluidRow(
                column(
                    6,
                    actionButton('update_Td', 'Actualitza', icon = icon("cloud-upload-alt")),
                    hr()
                )
            ),
            HTML("<b>Nota:</b> un cop actualitzades les temperatures es podran veure els canvis al cap de 10 minuts.")
        )
    })
    
    # Send MQTT message when push
    observeEvent(input$update_Td, {
        message("Actualitzant les Td")
        mqtt_messsage <- paste0('{"Td,c_winter": "', input$Td_winter,'", "Td,c_summer": "', input$Td_summer, '"}')
        iot$publish(                    
            topic='pump/control',
            qos=as.integer(1),
            payload=mqtt_messsage
        )
    })
    

    # Informació --------------------------------------------------------------

    output$table_config <- renderDataTable({
        config_tbl <- info()[, c('datetime', 'CONFIG', 'Operation_mode', 'Td,c_winter', 'Td,c_summer')]
        colnames(config_tbl) <- c('Dia i hora', 'Mode de configuració', "Mode d'operació", 'Consigna dipòsit hivern', 'Consigna dipòsit estiu')
        return( config_tbl )
    })
    
    output$table_funcionament <- renderDataTable({
        funcionament_tbl <- info()[, c('datetime', 'Alarm', 'State ON/OFF')]
        colnames(funcionament_tbl) <- c('Dia i hora', 'Alarma', 'Estat ON/OFF')
        return( funcionament_tbl )
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
