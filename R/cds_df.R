#' A cds_df constructor function from a tibble
#'
#' Possibly to export at a later stage, but now is used to maintain best
#' practices.
#'
#' @param x An R object.
cds_df <- function(x) {
  assert_that(
    inherits(x, "tbl_df"),
    all(c("cds_md_attributes", "cds_md_ent", "cds_md_enums_dicts") %in%
      names(attributes(x))),
    msg = "Only tibbles  with cds metadata can be made a cds_df!"
  )
  class(x) <- c("cds_df", class(x))
  x
}

#' Check if a tibble is a cds_df
#'
#' Checks whether a tibble is a cds_df downloaded via `download_cds()`.
#'
#' @param x An R object.
#'
#' @return A boolean scalar.
#' @export
is.cds_df <- function(x) {
  inherits(x, c("cds_df", "tbl_df")) &
    all(c("cds_md_attributes", "cds_md_ent", "cds_md_enums_dicts") %in%
      names(attributes(x)))
}

#' Get CDS metadata from cds_df tibbles.
#'
#' This function is a convienient getter function for accessing the attributes
#' of cds_df objects which contain CDS metadata for the entity from which a
#' cds_df was downloaded, as well as attributes and picklists. Use this
#' function instead of \code{base::attr()} or \code{base::attributes()}.
#'
#' @param cds_df A cds_df tibble, typically downloaded via
#' \code{cdsr::download_cds()}.
#' @param which_md A character scalar indicating what kind of CDS metadata you
#' would like to get from the \code{cds_df}. Must be one of the following:
#' "entity", "attributes" or "picklists". The default is "entity".
#'
#' @return A tibble, or list of tibbles in case of "picklists", containing the
#' requested CDS metadata information.
#'
#' @export
get_cds_md <- function(cds_df, which_md = "entity") {
  assert_that(
    is.cds_df(cds_df),
    msg = paste(
      "Provided object is not a cds_df. Are you sure it was downloaded via",
      "cdsR::download_cds()?")
  )

  md_key_map <- base::c(
    entity = "cds_md_ent",
    attributes = "cds_md_attributes",
    picklists = "cds_md_enums_dicts"
  )
  assert_that(
    which_md %in% names(md_key_map),
    msg = glue(
      "which_md argument has to be one of the following: {names(md_key_map)}")
  )

  requested_attr <- md_key_map[which_md]
  assert_that(
    requested_attr %in% names(attributes(cds_df)),
    msg = glue(
      "The requested attribute ({requested_attr}) is missing from the cds_df. ",
      "Are you sure it was created via cdsR::download_cds()?")
  )

  purrr::chuck(attributes(cds_df), requested_attr)
}

#' Parse cds_df data into native R data types.
#'
#' Had to be separeted from cds_df as otherwise it was caousing infinite loop
#' with dplyr's mutate. Also can be expanded for additional options.
#'
#' @param cds_df A cds_df to be typed.
type_cds_df <- function(cds_df) {

  typing_md_properties <- c("Format", "DateTimeBehavior", "@odata.type")
  attributes_md <- get_cds_md(cds_df, "attributes")

  # defense against entities with no fields to format
  if (!all(typing_md_properties %in% names(attributes_md))) return(cds_df)

  attributes_typing_md <-
    attributes_md %>%
    dplyr::select(LogicalName, AttributeType, typing_md_properties) %>%
    dplyr::filter(LogicalName %in% names(cds_df)) %>%
    dplyr::mutate(
      dplyr::across(
        DateTimeBehavior, purrr::modify_if, is.null,
        ~ list(Value = NA_character_)
      )
    ) %>%
    tidyr::unnest_wider(DateTimeBehavior) %>%
    dplyr::rename(DateTimeBehavior = Value) %>%
    dplyr::mutate(
      type = dplyr::case_when(
        DateTimeBehavior %in% c("UserLocal", "TimeZoneIndependent") ~
          "datetime",
        DateTimeBehavior %in% "DateOnly" ~ "date",
        stringr::str_detect(`@odata.type`, "MultiSelectPicklist") ~
          "multipicklist",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::filter(!is.na(type))

  if (nrow(attributes_typing_md) == 0) return(cds_df)

  .get_cols <- function(col_type) {
    attributes_typing_md %>%
      dplyr::filter(type %in% col_type) %>%
      dplyr::pull(LogicalName)
  }

  dplyr::mutate(
    cds_df,
    dplyr::across(.get_cols("datetime"), lubridate::as_datetime),
    dplyr::across(.get_cols("date"), lubridate::as_date),
    dplyr::across(.get_cols("multipicklist"),  strsplit, "; ")
  )
}

#' Evaluate a function without loosing CDS metadata attributes.
#'
#' Used primarly to implement S3 generics coming from dplyr which remove
#' CDS attributes and are popular (mainly mutate, and join variants).
#'
#' @param cds_df A cds_df tibble.
#' @param dplyr_f A function.
#' @param ... arguments passed to `dplyr_F`
#'
#' @return Output of the `dplyr_f` function evaluated with `...` with CDS
#' metadata attributes preserved.
evaluate_attrs <- function(cds_df, dplyr_f, ...) {
  cds_attrs <-
    attributes(cds_df) %>% .[stringr::str_detect(names(.), "^cds_md_")]

  tbl_df_evaluated <- dplyr_f(
    `class<-`(cds_df, class(cds_df) %>% .[. != "cds_df"]),
    ...
  )

  attributes(tbl_df_evaluated) <- base::append(
    attributes(tbl_df_evaluated),
    cds_attrs
  )

  cds_df(tbl_df_evaluated)
}

#' Use dplyr's join and mutate functions wihtout loosing CDS metadata attributes
#'
#' This function is an implementation of dplyr's mutate and join generic
#' functions for cds_df S3 object that are returned from `downloand_cds()`.
#' You don't have to worry abount anything and just use dplyr normally.
#'
#' This is needed due
#' to fact that `dplyr::mutate()` and joint do not preserve non-tibble
#' attributes. More information can be found
#' \href{https://github.com/tidyverse/dplyr/issues/1984}{here}.
#'
#' @param cds_df A cds_df tibble.
#' @param ... Arguments passed to dplyr mutate and join functions and their
#' variants.
#'
#' @name dplyr_drops_attributes
#' @export
mutate.cds_df <- function(cds_df, ...) evaluate_attrs(cds_df, mutate, ...)

#' @rdname dplyr_drops_attributes
#' @export
inner_join.cds_df <- function(cds_df, ...) evaluate_attrs(
  cds_df, inner_join, ...
)

#' @rdname dplyr_drops_attributes
#' @export
left_join.cds_df <- function(cds_df, ...) evaluate_attrs(cds_df, left_join, ...)

#' @rdname dplyr_drops_attributes
#' @export
right_join.cds_df <- function(cds_df, ...) evaluate_attrs(
  cds_df, right_join, ...
)

#' @rdname dplyr_drops_attributes
#' @export
full_join.cds_df <- function(cds_df, ...) evaluate_attrs(cds_df, full_join, ...)

#' @rdname dplyr_drops_attributes
#' @export
nest_join.cds_df <- function(cds_df, ...) evaluate_attrs(cds_df, nest_join, ...)


#' Parse enum type columns (picklists, states, etc.) to keys or labels.
#'
#' This function can be used to easily swap the labels or keys of enum fields
#' present in a cds_df. Especially useful when launching custom requests and
#' not using `update_cds_records()`.
#'
#' @param cds_df A cds_df tibble downloaded via `download_cds()`.
#' @param to A character scalar, either 'key' or 'label', defining what you
#' want to parse into. Default is 'key'.
#'
#' @return A cds_df with all present enum columns parsed either to it's key or
#' label, depending on the `to` argument.
#' @export
parse_cds_enums <- function(cds_df, to = "key") {
  to_options <- c("key", "label")
  assert_that(
    any(to %in% to_options),
    msg = "to argument has to be set either 'key' or 'label'"
  )
  enum_dicts <- get_cds_md(cds_df, "picklists")
  enum_cols <- names(enum_dicts) %>% .[. %in% names(cds_df)]

  if (length(enum_cols) == 0) {
    warning("No enums to parse, returning the initial cds_df")
    return(cds_df)
  }

  enums_type <-
    cds_df[, enum_cols] %>%
    purrr::map_chr(class) %>%
    purrr::map(
      ~ if (is.list(.x)) unique(purrr::map_chr(.x, class)) else class(.x)
    ) %>%
    unlist() %>%
    unique()
  assert_that(
    length(enums_type) == 1L,
    any(c("character", "numeric", "integer") %in% enums_type),
    if (enums_type == "character") to == "key" else to == "label",
    msg = paste(
      "Enums can only be parsed from character to integers and vice versa. ",
      "Ensure that all your enum columns are of the same type and that `to` ",
      "argument is set accordingly."
    )
  )

  from <- to_options %>% .[. != to]

  .parse_enum <- function(enum_data, enum_name) {
    enum_dict <- enum_dicts[[enum_name]]
    assert_that(
      enum_data %>%
        unlist() %>%
        .[!is.na(.)] %>%
        `%in%`(enum_dict[[from]]) %>%
        all(),
      msg = glue(
        "Enum column {enum_name} has {from} values not present in the ",
        "dictionary"
      )
    )
    # ifing for performence to benefit from vectorisation when possible
    if (is.list(enum_data)) {
      enum_data %>%
        purrr::modify_if(
          ~ !all(.x %in% NA),
          purrr::map_chr,
          ~ enum_dict[[to]][enum_dict[[from]] == .x]
        ) %>%
        purrr::modify_if(~ !all(.x %in% NA), paste, collapse = ",") %>%
        unlist()
    } else {
      dplyr::tibble(data = enum_data) %>%
        dplyr::rename_with(~ from, .cols = data) %>%
        dplyr::left_join(enum_dict, by = from) %>%
        dplyr::pull(to)
    }
  }

  dplyr::mutate(
    cds_df,
    dplyr::across(enum_cols, ~ .parse_enum(.x, dplyr::cur_column()))
  )
}

