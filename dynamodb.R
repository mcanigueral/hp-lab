library(reticulate)
library(config)
library(dplyr)
library(tidyr)
library(lubridate)
library(dygraphs)
library(dutils)

config <- config::get(file = 'config.yml')

# Python
reticulate::use_python(config$python_path, required = T) # Restart R session to change the python env
boto3 <- import("boto3")
dbKey <- import("boto3.dynamodb")$conditions$Key
dbAttr <- import("boto3.dynamodb")$conditions$Attr

# Database import --------------------------------------------------
dynamodb <- boto3$resource('dynamodb',
                           aws_access_key_id = config$dynamodb$access_key_id,
                           aws_secret_access_key = config$dynamodb$secret_access_key,
                           region_name = config$dynamodb$region_name)

table <- dynamodb$Table(config$dynamodb$table_hp)

response <- table$query(
  KeyConditionExpression = dbKey("day")$eq(as.character(Sys.Date()))
)

data <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
  mutate(datetime = with_tz(force_tz(as_datetime(paste(day, time)), tzone = 'CEST'), tzone = 'Europe/Madrid')) %>% 
  select(datetime, everything(), -day, -time)

temperatures <- data[, c(1, 4, 11, 12, 14, 16, 17)]



temperatures %>% 
  df_to_ts() %>% 
  dygraph() %>% 
  format_dygraph()




# Libelium ----------------------------------------------------------------

table_libelium <- dynamodb$Table('LibeliumMeasures')

response <- table_libelium$scan(
  FilterExpression = dbAttr("ts")$between(as.integer(as_datetime(Sys.Date()-days(2))), as.integer(as_datetime(Sys.Date())))
)

data <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
  mutate(datetime = with_tz(as_datetime(ts), tzone = 'Europe/Madrid')) %>% 
  select(datetime, Tint) %>% 
  arrange(datetime)



# Modify setpoints table values -----------------------------------------------------

table_control <- dynamodb$Table(config$dynamodb$table_control)

response <- table_control$scan()

hp_control <- map_dfr(response$Items, ~ as_tibble(parse_item(.x))) %>% 
  rename(day_type = `day-type`) %>% 
  select(day_type, hour, setpoint, speed)











