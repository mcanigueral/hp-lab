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
pd <- import("pandas")
# dynamodb_filter <- import("boto3.dynamodb")$conditions
dbKey <- import("boto3.dynamodb")$conditions$Key
dbAttr <- import("boto3.dynamodb")$conditions$Attr

# Database import --------------------------------------------------
dynamodb <- boto3$resource('dynamodb',
                           aws_access_key_id = config$dynamodb$access_key_id,
                           aws_secret_access_key = config$dynamodb$secret_access_key,
                           region_name = config$dynamodb$region_name)

table <- dynamodb$Table(config$dynamodb$table_hp)
table_libelium <- dynamodb$Table(config$dynamodb$table_meshlium)



# UI ----------------------------------------------------------------------
ui <- fluidPage(
    titlePanel("Bomba de calor lab eXiT"),
    sidebarLayout(
        sidebarPanel = sidebarPanel(
            dateInput("date", "Dia a visualitzar:", 
                      value = Sys.Date(),
                      min = dmy(01122020), max = Sys.Date()),
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
           fluidRow(
               dygraphOutput("plot")
           ),
           hr()
        )
    )
)


# Server ------------------------------------------------------------------
server <- function(input, output) {
    
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
    
    output$download  <- downloadHandler(
        filename = function() {paste0("hp_lab_", Sys.Date(), ".xlsx")},
        content = function(file) {
            writexl::write_xlsx(total_data(), path = file)
        }
    )
}

# Run the application 
shinyApp(ui = ui, server = server)
