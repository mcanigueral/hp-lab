
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