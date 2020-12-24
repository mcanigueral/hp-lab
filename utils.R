
# Reticulate utils --------------------------------------------------------

parse_object <- function(object) {
  if ('decimal.Decimal' %in% class(object)) {
    as.numeric(as.character(object))
  } else if ('character' == class(object)) {
    as.character(object)
  } else if ('logical' == class(object)) {
    as.logical(object)
  }
}

parse_item <- function(values_list) {
  map(values_list, ~ parse_object(.x))
}



# Inputs table ------------------------------------------------------------

tableInput <- function(tbl, partitionKey) {
  fluidRow(
    column(
      4,
      h5("Temperatura (ºC)"),
      pmap(
        filter(tbl, day_type == partitionKey),
        ~ numericInput(paste0('setpoint', '_', ..1, '_', ..2), NULL, ..3)
      )
    ),
    column(
      4,
      h5("Ventilador"),
      pmap(
        filter(tbl, day_type == partitionKey),
        ~ numericInput(paste0('speed', '_', ..1, '_', ..2), NULL, ..4)
      )
    )
  )
}

get_control_value <- function(hp_control, input_name) {
  input <- unlist(strsplit(input_name, '_'))
  hp_control[[input[1]]][
    hp_control[['day_type']] == input[2] & hp_control[['hour']] == as.integer(input[3])
  ]
}

update_control_value <- function(dynamodb_table, input_name, value) {
  input <- unlist(strsplit(input_name, '_'))
  value_list <- list(dict("Value" = as.integer(value)))
  names(value_list) <- input[1]
  dynamodb_table$update_item(
    Key = dict(list("day-type" = input[2],  "hour" = as.integer(input[3]))),
    AttributeUpdates = dict(value_list)
  )
}
