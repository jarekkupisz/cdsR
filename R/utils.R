#   ____________________________________________________________________________
#   unnest_wider_all() for auto expanding all these deeply nested JSONS
#   It is needed as tidyr::unnest_wider() cannot do it on multiple cols/levels
unnest_wider_all <- function(df) {

  cols_unnestable <- character()
  get_list_col_name <- function() {
    df %>%
      purrr::map_lgl(base::is.list) %>%
      .[.] %>%
      base::names() %>%
      .[!. %in% cols_unnestable] %>%
      purrr::pluck(1, .default = character())
  }

  col_list_name <- get_list_col_name()
  if (base::length(col_list_name) == 0) return(df)

  while (base::length(col_list_name) > 0) {
    col_list <- df[[col_list_name]]

    if (purrr::map_lgl(col_list, ~ is.null(names(.x))) %>% all()) {
      cols_unnestable <- c(cols_unnestable, col_list_name)
      col_list_name <- get_list_col_name()
      next
    }

    if (any(purrr::map_lgl(col_list, ~ is.null(names(.x))))) {
      filler <-
        purrr::keep(col_list, ~ !is.null(names(.x))) %>%
        .[[1]] %>%
        purrr::map(~ NA)

      df <- dplyr::mutate_at(
        df, col_list_name,
        purrr::modify_if, ~ is.null(names(.x)), ~ filler
      )
    }

    df <- tidyr::unnest_wider(df, col_list_name, names_sep = ".")
    col_list_name <- get_list_col_name()
  }

  df
}
