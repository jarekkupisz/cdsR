#' Set authentication token(s) used by other cdsR functions.
#'
#' This is the first function you need to call in order to work with other
#' functions in cdsR. Based on provided credentials it sets up a cds_token S3
#' generic object. It then lives in R session options
#' (\code{getOption("cdsR.tokens")}).To get credentials you have to have a
#' service account. Ask your adminds to provide you with all the secrets or get
#' more details \href{https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/authenticate-oauth}{here}.
#'
#' This is a very lighweight approach to authentication and is designed to
#' minimise typing in interactive sessions, while still allowing for writing
#' explicit, yet concise production-ready code. Notable features:
#' \itemize{
#'   \item All the tokens in \code{getOption("cdsR.tokens")} are automatically
#'   refreshed if token is valid for less than 10 minutes when launching the
#'   odata request.
#'   \item It is common to work with multiple instances or tenants of CDS or
#'   Dynamics. You can call `initialize_cds_token()` as many times as you want,
#'   just make sure that each new instance/tenant you are working with has
#'   unique `cds_instance_name`. The typicall scenario is to use the cdsR in
#'   production, testing and development environments just by switching one
#'   argument.
#'   \item If just one token is present, then `donwload_cds()` picks it up
#'   automatically and you do not need to provide the `cds_instance_name`. Note
#'   that function which send destructive data requests (deletes and updates)
#'   require to provide instance name.
#' }
#' Note that you can call this function for a cds_instance_name in order to
#' force refresh the token and/or change the url details.
#'
#' @param cds_instance_name A character scalar with the name you want to use in
#' order to identify a particular CDS instance. Typically something like
#' "production", "test" or "dev". Used in other cdsR functions.
#' @param cds_instance_url A character scalar with the url of your CDS/Dynamics
#' instance. Should look like "https://yourinstancename.crm4.dynamics.com"
#' @param tenant_id A character scalar with GUID of your tenant.
#' Ask your Dynamics admins to provide this when in doubt.
#' @param client_id A character scalar with GUID of service account you
#' will use with this CDS instance. Ask your Dynamics admins to provide this
#' when in doubt.
#' @param client_secret A character scalar representing the service account's
#' secret. Avoid keeping this in your code! Ask your dynamics admins to provide
#' when in doubt and consult with your org's security expert how to store.
#' @param web_api_version A character scalar describing which version of the
#' CDS Web API you want to use when using this `cds_token`. Default is "9.0".
#'
#' @return This function returns nothing and is used for side-effect of creating
#' a cds_token object in R session's options to use by other cdsR functions.
#'
#' @export
initialize_cds_token <- function(cds_instance_name,
                              cds_instance_url,
                              tenant_id,
                              client_id,
                              client_secret,
                              web_api_version = "9.1") {

  ##  ..........................................................................
  ##  Getting the token                                                     ####
  token_request <- httr::POST(
    paste0("https://login.microsoftonline.com/", tenant_id, "/oauth2/token"),
    body = list(
      client_id = client_id,
      client_secret = client_secret,
      resource = cds_instance_url,
      grant_type = "client_credentials"),
    encode = "form"
  )
  token_request_reply <- httr::content(token_request)

  if (httr::http_error(token_request)) return(token_request_reply)

  ##  ..........................................................................
  ##  Contructing the token and setting it to options                       ####
  token <- structure(
    list(
      name = cds_instance_name,
      web_api_url = cds_instance_url %>%
        stringr::str_remove("/$") %>%
        paste0("/api/data/v", web_api_version, "/"),
      token = purrr::chuck(token_request_reply, "access_token"),
      expires_on = token_request_reply %>%
        purrr::chuck("expires_in") %>%
        as.integer() %>%
        `+`(Sys.time()),
      tenant_id = tenant_id,
      client_id = client_id,
      client_secret = client_secret),
    class = "cds_token"
  )

  options(
    cdsR.tokens =
      `[[<-`(as.list(getOption("cdsR.tokens")), cds_instance_name, token)
  )
  print(glue(
    "Token set in options('cdsR.tokens') for {cds_instance_name} ",
    "({cds_instance_url})"
  ))
}

#   ____________________________________________________________________________
#   get_cds_token() to get a token and if needed, refresh it, internal    ####
get_cds_token <- function(cds_instance_name = NULL) {

  ##  ..........................................................................
  ##  When null is left as the arguemt, get it if only one exists           ####
  tokens_cached <- getOption("cdsR.tokens")

  if (is.null(cds_instance_name)) {
    assert_that(
      !is.null(tokens_cached),
      msg = paste(
        "No tokens found in options('cdsR.tokens'). Please initialize one by",
        "calling cdsR::initialize_cds_token() with all needed credentials.")
    )
    assert_that(
      length(tokens_cached) == 1,
      msg = paste(
        "Multiple tokens found (verify with getOption('cdsR.tokens')).",
        "Please provide the cds_instance_name argument.")
    )
    cds_instance_name <- names(tokens_cached)
  }

  ##  ..........................................................................
  ##  Gettnig the token from cache or refresh                               ####
  token_cached <- getOption("cdsR.tokens")[[cds_instance_name]]
  assert_that(
    !is.null(token_cached) & inherits(token_cached, "cds_token"),
    msg = glue(
      "No token found in options('cdsR.tokens') for name ",
      "'{cds_instance_name}'. Please initialize one by calling",
      "cdsR::initialize_cds_token() with all needed credentials.")
  )

  token_cached_valid_for_mins <-
    token_cached$expires_on %>%
    difftime(Sys.time(), units = "mins") %>%
    as.integer()

  if (token_cached_valid_for_mins >= 10) token_cached else {
    initialize_cds_token(
      cds_instance_name,
      cds_instance_url = token_cached$web_api_url %>%
        stringr::str_sub(end = min(stringr::str_locate(., "api/data")) - 1),
      tenant_id = token_cached$tenant_id,
      client_id = token_cached$client_id,
      client_secret = token_cached$client_secret,
      web_api_version = token_cached$web_api_url %>%
        stringr::str_sub(nchar(.) - 3, nchar(.) - 1)
    )
    get_cds_token(cds_instance_name)
  }
}

#   ____________________________________________________________________________
#   get_cds_headers() to consisely construct requests, internal             ####
get_cds_headers <- function(cds_instance_name = NULL) {
  token <- get_cds_token(cds_instance_name)
  httr::add_headers(
    `OData-MaxVersion` = "4.0",
    `OData-Version` = "4.0",
    Authorization = paste("Bearer", token$token),
    `Content-Type` = "application/json; charset=utf-8",
    Accept = "application/json",
    Prefer =
      "odata.include-annotations=\"OData.Community.Display.V1.FormattedValue\""
  )
}


