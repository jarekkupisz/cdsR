#   ____________________________________________________________________________
#   download_cds_raw() for returning the content in spceidifed type         ####
#   This function is then used to either download or serialize in other funs.
#   It also handles adding tokens to env and refreshing them if needed.
download_cds_raw <- function(query,
                             cds_instance_name = NULL,
                             content_type = NULL) {

  ##  ..........................................................................
  ##  Downloading the result                                                ####
  query_full <- dplyr::if_else(
    stringr::str_detect(query, "\\$skiptoken="),
    query,
    paste0(get_cds_token(cds_instance_name)$web_api_url, query)
  )
  reply_content <- httr::content(
    httr::GET(utils::URLencode(query_full), get_cds_headers(cds_instance_name)),
    type = content_type,
    encoding = "UTF-8"
  )
  assert_that(
    all(names(reply_content) != "error"),
    msg = paste("Request resulted in an error: ", reply_content$error$message)
  )

  reply_content
}

#   ____________________________________________________________________________
#   download_cds_md() for getting the metadata to contruct cds_df           ####
download_cds_md <- function(ent_name,
                            is_ent_name_plural = FALSE,
                            cds_instance_name = NULL) {

  cat(paste("\n", glue("Downloading {ent_name} CDS metadata...")))
  ent_name_type <- ifelse(is_ent_name_plural, "EntitySetName", "LogicalName")

  md <-
    glue(
      "EntityDefinitions?$filter={ent_name_type}%20eq%20'{ent_name}'",
      "&$expand=Attributes") %>%
    download_cds_raw(cds_instance_name) %>%
    purrr::chuck("value") %>%
    {tidyr::tibble(md = .)} %>%
    tidyr::unnest_wider(md)

  assert_that(
    nrow(md) == 1,
    msg = glue(
      "Incorrect entity metadata downloaded. Esnure that '{ent_name}' ",
      "is a valid {ent_name_type} for an entity in ",
      "{tkn$name} ({tkn$web_api_url}).", tkn = get_cds_token(cds_instance_name)
    )
  )

  attributes_md <-
    md %>%
    dplyr::select(Attributes) %>%
    tidyr::unnest_longer(Attributes) %>%
    tidyr::unnest_wider(Attributes)

  list(md = md, attributes_md = attributes_md)
}

#' Download data from CDS as a formatted tibble with metadata.
#'
#' This is the main function of the package which allows for easy downloading
#' of data from CDS into your R session. It provides the CDS metadata via
#' attributes "md_ent" and "md_attributes" as tibbles as well, so you can
#' benefit from it when automatizing the analytics tasks.
#'
#' Tibbles returned by this function have several properties
#' \itemize{
#'   \item All enum type fields (picklists, status and state) are returned as
#'   proper character vectors containing labels, not the integer keys that
#'   the Web API normally returns.
#'   \item In "md_ent" attribute (\code{attr(a_cds_df, "md_ent")}) you can find
#'   all entity metadata listed \href{https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/entitymetadata?view=dynamics-ce-odata-9}{here}.
#'   \item In "md_attributes" attribute (\code{attr(a_cds_df, "md_attributes")})
#'   you can find all details regarding returned entitiy's attributes. The
#'   full list can be found \href{https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/attributemetadata?view=dynamics-ce-odata-9}{here}.
#'   \item In "md_enums_dicts" attribute
#'   (\code{attr(a_cds_df, "md_enums_dicts")}) you can find list of tibbles
#'   representing key, value pairs for enums fields which were used to parse
#'   them
#' }
#'
#' @param query A character scalar with the odata request for which you wish
#' to download a result. Typically it will be a plural logical name of an entity
#' ("contacts", "accounts", "phonecalls", etc.). It is passed directly in a
#' request so complex queries with odata operators are possible. The only
#' condition is that it has to perform a retrieve operation on a main entity.
#' @param cds_instance_name A character scalar used in
#' \code{initialize_cds_token()} to name a CDS instance. Default NULL. If only
#' one cds_token object is present in \code{getOption("cdsR.tokens")}, then you
#' do not need to specify this argument.
#' @param select A string or vector of strings with names of attributes you wish
#' to select from an entity. If  a vector, all values will be collapsed
#' using "," as separator.
#' @param filter A string or vector of strings with odata filter statements.
#' If vector, all values will be collapsed using " and " simmilarly to dplyr's
#' filter.
#' @param  order_by A string or vector of strings expressing entity fields
#' by which you wish to arrange the result. If  a vector, all values will be
#' collapsed using "," as separator.
#' @param top A scalar integer determining how many rows of the base entity you
#' wish to retrieve.
#'
#' @return A cds_df S3 object, which is essentially a tibble with additional
#' attributes described in the \emph{Details} section.
#'
#' @export
download_cds <- function(query,
                         cds_instance_name = NULL,
                         select = NULL,
                         filter = NULL,
                         order_by = NULL,
                         top = NULL) {

  ##  ..........................................................................
  ##  Constructing the query from arguments if provided                     ####
  if (!stringr::str_detect(query, "\\?") &
      !all(list(select, filter, order_by, top) %>% purrr::map_lgl(is.null))) {

    if (!is.null(top)) assert_that(
      assertthat::is.count(top),
      msg = "'top' has to be an integer scalar"
    )

    odata_operators_query_part <-
      dplyr::tribble(
        ~odata_operator,  ~user_args, ~separator,
        "$select=",       select,     ",",
        "$filter=",       filter,     " and ",
        "$orderby=",      order_by,   ",",
        "$top=",          top,        ""
      ) %>%
      dplyr::rowwise() %>%
      dplyr::filter(!is.null(user_args)) %>%
      dplyr::mutate(
        query_part = paste0(
          odata_operator,
          paste(user_args, collapse = separator)
        )
      ) %>%
      dplyr::pull(query_part) %>%
      paste(collapse = "&")

    query <- paste0(query, "?", odata_operators_query_part)
  }

  ##  ..........................................................................
  ##  Functions for requests                                                ####
  .download_cds_raw <- function(.query = query, ...) download_cds_raw(
    .query, cds_instance_name, ...
  )
  .format_reply <- function(httr_content) {
    httr_content %>%
      purrr::chuck("value") %>%
      purrr::map(function(.x) `[<-`(.x, sapply(.x, is.null), NA)) %>%
      dplyr::bind_rows() %>%
      dplyr::relocate(dplyr::one_of("@odata.etag"), .after = dplyr::last_col())
  }

  ##  ..........................................................................
  ##  Downloading and returning if 0 or just one page of results present    ####
  token <- get_cds_token(cds_instance_name)
  print(glue("Downloading {query} from {token$name} ({token$web_api_url})"))

  reply_content <- .download_cds_raw()

  if (reply_content$value %>% {class(.) == "list" & length(.) == 0}) {
    warning("Empty result downloaded, returning NULL")
    return(NULL)
  }

  result <- .format_reply(reply_content)

  ##  ....................................................... ..................
  ##  Binding all next results page                                         ####
  if ("@odata.nextLink" %in% names(reply_content)) {
    all_pages_downloaded <- F
    page_indx <- 2
    next_link <- reply_content$`@odata.nextLink`
    results_list <- list(result)

    while (!all_pages_downloaded) {
      cat("\r", glue("Downloading result page {page_indx}"))

      .reply_content <- .download_cds_raw(next_link)
      results_list <- append(results_list, list(.format_reply(.reply_content)))

      page_indx <- page_indx + 1
      next_link <- .reply_content$`@odata.nextLink`
      if (!"@odata.nextLink" %in% names(.reply_content)) {
        all_pages_downloaded <- T
      }
    }
    result <- dplyr::bind_rows(results_list)
  }

  ### . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ..
  ### Parsing the pesky reference column names                              ####
  names(result) <-
    names(result) %>%
    purrr::modify_if(
      stringr::str_detect(., "^_[:graph:]*_value($|@)"),
      ~stringr::str_remove(.x, "_value") %>% stringr::str_remove("^_"))

  ##  ..........................................................................
  ##  Adding Entity MD Download as an Attribute                             ####
  name_ent_plural <-
    query %>%
    purrr::modify_if(
      stringr::str_detect(., "\\?"),
      ~ stringr::str_sub(.x, end = stringr::str_locate(.x, "\\?")[1, 1] - 1)
    )
  mds <- download_cds_md(name_ent_plural, TRUE, cds_instance_name)
  md <- mds$md
  md_attributes <- mds$attributes_md

  ##  ..........................................................................
  ##  Formating enums and lookup attributes                                 ####
  md_enums_dicts <- list()
  to_format_columns_md <-
    md_attributes %>%
    dplyr::filter(
      AttributeType %in% c("Picklist", "State", "Status", "Lookup", "Owner") |
        stringr::str_detect(`@odata.type`, "MultiSelectPicklist"),
      LogicalName %in% names(result)
    ) %>%
    # multiselects do not have their own type, so constructing my own, MS 🤦
    dplyr::mutate(dplyr::across(
      AttributeType,
      ~ dplyr::if_else(
        .x %in% "Virtual" &
          stringr::str_detect(`@odata.type`, "MultiSelectPicklist"),
        "MultiSelectPicklist",
        .x
      )
    )) %>%
    dplyr::select(LogicalName, AttributeType)

  to_format_enums <-
    to_format_columns_md %>%
    dplyr::filter(
      AttributeType %in% c("Picklist", "State", "Status", "MultiSelectPicklist")
    ) %>%
    dplyr::pull(LogicalName)

  for (enum in to_format_enums) {
    cat("\r", paste("Downloading MD for enum type attribute:", enum))

    enum_type <-
      to_format_columns_md %>%
      dplyr::filter(LogicalName == enum) %>%
      dplyr::pull(AttributeType)
    enum_dict <-
      glue(
        "EntityDefinitions(LogicalName='{md$LogicalName}')/",
        "Attributes(LogicalName='{enum}')/",
        "Microsoft.Dynamics.CRM.{enum_type}AttributeMetadata?",
        "$select=LogicalName&$expand=OptionSet,GlobalOptionSet") %>%
      .download_cds_raw() %>%
      purrr::compact() %>%
      purrr::chuck(ifelse(
        "GlobalOptionSet" %in% names(.), "GlobalOptionSet", "OptionSet"
      )) %>%
      purrr::chuck("Options") %>%
      {tidyr::tibble(.picklist_dict = .)} %>%
      tidyr::unnest_wider(.picklist_dict) %>%
      tidyr::unnest_wider(Label, names_sep = ".") %>%
      tidyr::unnest_wider(Label.UserLocalizedLabel, names_sep = ".")

    md_enums_dicts[[enum]] <-
      enum_dict %>%
      dplyr::select(key = Value, label = Label.UserLocalizedLabel.Label)
  }

  odata_annot_suffix <- "@OData.Community.Display.V1.FormattedValue"
  to_format_columns <-
    list(
      enums = to_format_enums,
      lookups =  to_format_columns_md$LogicalName %>% .[!. %in% to_format_enums]
    ) %>%
    purrr::map(~ .x[paste0(.x, odata_annot_suffix) %in% names(result)])

  non_empty_unparsed_result <-
    result %>%
    dplyr::select(dplyr::all_of(
      to_format_columns_md %>%
        dplyr::filter(
          !LogicalName %in% purrr::flatten_chr(to_format_columns),
          !stringr::str_detect(LogicalName, "^owning")) %>%
        dplyr::pull(LogicalName)
    )) %>%
    tidyr::drop_na()

  if(non_empty_unparsed_result %>% dim() %>% `!=`(0) %>% all()) warning(paste(
      "Non-empty unparsed enum or lookup attributes:",
      paste(names(non_empty_unparsed_result), collapse = ",")
  ))

  result <-
    result %>%
    dplyr::select(-dplyr::all_of(to_format_columns$enums)) %>%
    dplyr::rename_with(
      .fn = ~ stringr::str_remove(.x, paste0(odata_annot_suffix, "$")),
      .cols =
        dplyr::any_of(paste0(to_format_columns$enums, odata_annot_suffix))
    ) %>%
    dplyr::rename_with(
      .fn =
        ~ stringr::str_replace(.x, paste0(odata_annot_suffix, "$"), ".display"),
      .cols =
        dplyr::any_of(paste0(to_format_columns$lookups, odata_annot_suffix))
    ) %>%
    #this drops for eg. date attributes that we want to format in type_cds_df()
    dplyr::select(-dplyr::ends_with(odata_annot_suffix))

  ##  ..........................................................................
  ##  Parsin the owning attributes, which are not retrieved by headers      ####
  owning_columns_md <-
    md_attributes %>%
    dplyr::filter(
      stringr::str_detect(LogicalName, "^owning"),
      AttributeType %in% "Lookup",
      !IsCustomAttribute,
      LogicalName %in% names(result),
      !LogicalName %in% to_format_columns$lookups
    ) %>%
    dplyr::select(LogicalName, dplyr::any_of("Targets")) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of("Targets"),
        ~ if (identical(.x, list())) character() else unlist(.x)
      )
    )

  if (nrow(owning_columns_md) > 0) {
    owning_columns_md <- dplyr::left_join(
      owning_columns_md,
      suppressWarnings(
        owning_columns_md %>%
          dplyr::pull(Targets) %>%
          {glue("LogicalName eq '{.}'")} %>%
          paste(collapse = " or ") %>%
          {paste0(
            "EntityDefinitions?$filter=", .,
            "&$select=EntitySetName,PrimaryNameAttribute,LogicalName,",
            "PrimaryIdAttribute")} %>%
          .download_cds_raw() %>%
          .format_reply()
      ),
      by = c("Targets" = "LogicalName")
    )
  }

  for (owning_column in owning_columns_md %>% split(.$LogicalName)) {
    cat("\r", paste("Downloading entity ", owning_column$Targets))

    owning_ent <- purrr::quietly(download_cds)(
      owning_column %>%
        {paste0(.$EntitySetName, "?$select=", .$PrimaryNameAttribute)},
      cds_instance_name
    )[["result"]]

    .col_name <- owning_column$LogicalName
    result <- dplyr::left_join(
      result,
      owning_ent %>%
        dplyr::select(
          owning_column$PrimaryIdAttribute,
          owning_column$PrimaryNameAttribute) %>%
        dplyr::rename_with(~ .col_name, 1) %>%
        dplyr::rename_with(~ paste0(.col_name, ".display"), 2),
      by = .col_name
    )

    if (any(is.na(result[[paste0(.col_name, ".display")]])) &
          all(!is.na(result[[.col_name]]))) warning(
      paste("There are no existing owning records for", .col_name)
    )

  }

  ##  ..........................................................................
  ##  Returning the result df with attributes                               ####
  attr(result, "cds_md_enums_dicts") <- md_enums_dicts
  attr(result, "cds_md_ent") <- md %>% dplyr::select(-Attributes)
  attr(result, "cds_md_attributes") <- md_attributes

  type_cds_df(cds_df(result))
}

#' Conveniently include CDS Web API filter functions in download_cds()
#'
#' CDS Web API offers a variety of filtering functions that are not standard
#' in Odata. They are very useful when it comes to limiting the number of rows
#' you might want to retrieve from an entity. The problem is that writing
#' them in a query is annoying to say at least. This function will generate a
#' string that callse the specified filter function wirhout need to encapsulate
#' strings in ticks or remembering how arrays of values are written in odata.
#' See list of available filter functions here:
#' \href{https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/queryfunctions?view=dynamics-ce-odata-9}{Web API Query Function Reference}
#'
#' @param cds_fun_name A string representing the name of the filter function.
#' Available functions: \href{https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/queryfunctions?view=dynamics-ce-odata-9}{Web API Query Function Reference}
#' @param PropertyName Most filter functions work on at least one field from the
#' base entity in your query. This argument allows you to specify this column as
#' an R string. If a function does not use any fields from an entity, leave is as
#' NULL (default value).
#' @param ... If a query function uses more arguments than just PropertyName,
#' you can specify them here. As argument names use the same names as you can
#' find in the documentation.
#'
#' @return A string that represents CDS Web API Odata call that can be used as
#' argument in the Odata $filter system function. See usage examples.
#'
#' @examples
#' cds_filter_fun("On", PropertyName = "createdon", PropertyValue = "2020-02-21")
#' cds_filter_fun("Between", "sale", PropertyValues = c(100, 300))
#' \dontrun{
#' download_cds(
#'   "audits",
#'   filter = cds_filter_fun("LastXFiscalYears", "createdon", PropertyValue = 2)
#' )
#' }
#'
#' @export

cds_filter_fun <- function(cds_fun_name, PropertyName = NULL, ...) {

  .tickify <- function(chars) {
    if (is.character(chars))
      chars <- paste0("'", chars, "'")

    if (length(chars) > 1)
      chars <- paste0("[", paste(chars, collapse = ","), "]")

    as.character(chars)
  }

  property_value_args <- purrr::when(
    list(...),
    length(.) == 0 ~ NULL,
    TRUE ~
      purrr::imap_chr(
        .,
        ~ list(.y, .x) %>%
          purrr::modify_at(2, .tickify) %>%
          paste(collapse = "=")
      ) %>%
      paste(collapse = ",") %>%
      {paste0(",", .)}
  )

  if (is.null(PropertyName)) assert_that(
    length(property_value_args) != 0,
    msg = paste(
      "PropertyName and PropertyValue (...) arguments cannot be empty at the",
      "same time. Please provide PropertyName."
    )
  )

  property_name <- ifelse(
    is.null(PropertyName),
    NULL,
    paste0("PropertyName=", .tickify(PropertyName))
  )

  paste0(
    "Microsoft.Dynamics.CRM.",
    cds_fun_name, "(",
    property_name,
    property_value_args, ")"
  )
}

