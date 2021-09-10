#' Delete records from a CDS instance based on a cds_df.
#'
#' This function will send chained DELETE requests for each record
#' found in the provided in the cds_df. USE WITH CAUTION.
#'
#' If not all request resulter in success, a warning is displayed. You can
#' assign the result of this function to investigate.
#'
#' @param cds_df A cds_df created via calling \code{download_cds()} filtered
#' for records you want to delete from CDS.
#' @param cds_instance_name A character scalar used in
#' \code{initialize_cds_token()} to name a CDS instance.
#' @param fail_safe_entity_name A character scalar, bu default NULL. This
#' argument is here to avoid calling this function accidentally in you analysis
#' workflow. You need to provide the logical name of an entity from which you
#' want to remove records (e.g. "contact", "task"). By default NULL to display
#' a usefull warning instead of generic R's warning about an undefined
#' parameter.
#' @param force_operation A logical scalar indicating whether the optimistic
#' concurrency check should be performed. This ensuers that you are not deleting
#' records which changes after you downloaded them. Set to FALSE only if you
#' know what you are doing! More info on optimistic concurrency control in the
#' Web API can be found
#' \href{https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/perform-conditional-operations-using-web-api#bkmk_limi}{here}.
#'
#' @return List of httr requests objects for each record deletion. Useful to
#' investigate whether all requests went through and repeat for those which did
#' not.
#'
#' @export
delete_cds_records <- function(cds_df,
                               cds_instance_name,
                               fail_safe_entity_name = NULL,
                               force_operation = FALSE) {
  force(cds_instance_name) # to display right error to users
  md_ent <- get_cds_md(cds_df)
  ent_primary_id_attr <- dplyr::pull(md_ent, PrimaryIdAttribute)

  assert_that(
    !is.null(fail_safe_entity_name),
    fail_safe_entity_name %in% md_ent$LogicalName,
    msg = paste(
      "To ensure no accidental record removal, please provide the",
      "fail_safe_entity_name argument with value equal to the entity's",
      "logical name.")
  )
  is_cds_df_valid_for_upload(cds_df, check_etags = !force_operation)

  ##  ..........................................................................
  ##  Sending the delete requests                                           ####
  token <- get_cds_token(cds_instance_name)
  pb <- dplyr::progress_estimated(nrow(cds_df))

  .send_delete_request <- function(.cds_df) {
    .request <- httr::DELETE(
      glue(
        "{token$web_api_url}{md_ent$EntitySetName}",
        "({.cds_df[[ent_primary_id_attr]]})"),
      purrr::when(
        get_cds_headers(cds_instance_name),
        !force_operation ~
          c(., httr::add_headers(`If-Match` = .cds_df$`@odata.etag`)),
        force_operation ~ .)
    )
    pb$tick()$print()
    .request
  }

  print(glue(
    "Deleting {nrow(cds_df)} records in {md_ent$LogicalName} ",
    "entity on {token$name} ({token$web_api_url})."
  ))
  requests <-
    cds_df %>%
    base::split(.[[ent_primary_id_attr]]) %>%
    purrr::map(.send_delete_request)

  requests_errors_n <- purrr::keep(requests, httr::http_error) %>% length()
  if (requests_errors_n > 0) warning(glue(
    "There were {requests_errors_n} requests with errors. Please review the ",
    "resulting list to troubleshoot."
  ))

  requests
}

#' Update records easily directly from a cds_df.
#'
#' This function sends chained patch requests for each record found in the
#' cds_df. These records have to be present at the moment of executing the
#' underlying request. It will update all the fields present in the cds_df.
#' The typical workflow for this function is to filter for records you are
#' interested in, update the column values, select these columns and at the end
#' of the pipe launch this function.
#'
#' @param cds_df A cds_df with records and columns for which you wish to
#' update values in you CDS. It will be parsed using
#' `parse_cds_enums(cds_df, to = 'key')`
#' @param cds_instance_name A character scalar used in
#' \code{initialize_cds_token()} to name a CDS instance.
#' @param fail_safe_entity_name A character scalar, bu default NULL. This
#' argument is here to avoid calling this function accidentally in you analysis
#' workflow. You need to provide the logical name of an entity from which you
#' want to remove records (e.g. "contact", "task"). By default NULL to display
#' a usefull warning instead of generic R's warning about an undefined
#' parameter.
#' @param force_operation A logical scalar indicating whether the optimistic
#' concurrency check should be performed. This ensures that you are not changing
#' records which changed after you downloaded them. Set to FALSE only if you
#' know what you are doing! More info on optimistic concurrency control in the
#' Web API can be found
#' \href{https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/perform-conditional-operations-using-web-api#bkmk_limi}{here}.
#' @param exclude_null_cols A logical scalar indicating whether columns with
#' nulls should be included in a single record PATCH request. Especially useful
#' if you have null lookup field in your data for certain records and you want
#' to exclude just the to prevent the whole request from failing.
#'
#' @return List of httr requests objects for each record update. Useful to
#' investigate whether all requests went through and repeat for those who did
#' not.
#'
#' @note
#' All resulting PATCH requests prevent upsert operation by using the
#' \code{If-Match: *} header, even if \code{force_operation = TRUE}. See more
#' info about upsert operation in the Dataverse Web API
#' \href{https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/perform-conditional-operations-using-web-api#bkmk_limitUpsertOperations}{here}.
#'
#' @export
update_cds_records <- function(cds_df,
                               cds_instance_name,
                               fail_safe_entity_name = NULL,
                               force_operation = FALSE,
                               exclude_null_cols = FALSE) {
  ##  ..........................................................................
  ##  Assertions and MD setup                                               ####
  force(cds_instance_name)

  md_ent <- get_cds_md(cds_df, "entity")
  ent_primary_id_attr <- dplyr::pull(md_ent, PrimaryIdAttribute)

  assert_that(
    !is.null(fail_safe_entity_name),
    fail_safe_entity_name %in% md_ent$LogicalName,
    msg = paste(
      "To ensure no accidental record changes, please specify the",
      "fail_safe_entity_name argument with value equal to the entity's",
      "logical name.")
  )
  is_cds_df_valid_for_upload(cds_df, check_etags = !force_operation)

  ##  ..........................................................................
  ##  Sending the patch request                                             ####
  token <- get_cds_token(cds_instance_name)
  pb <- dplyr::progress_estimated(nrow(cds_df))

  .send_patch_request <- function(.cds_result) {
    .request <- httr::PATCH(
      glue(
        "{token$web_api_url}{md_ent$EntitySetName}",
        "({.cds_result[[ent_primary_id_attr]]})"),
      purrr::when(
        c(
          get_cds_headers(cds_instance_name),
          httr::add_headers(`If-Match` = "*")),
        !force_operation ~
          c(., httr::add_headers(`If-Match` = .cds_result$`@odata.etag`)),
        force_operation ~ .),
      body = .cds_result %>%
        dplyr::select(-ent_primary_id_attr) %>%
        {
          if(exclude_null_cols) dplyr::select(., -where(~ all(is.na(.x))))
          else .
        } %>%
        jsonlite::unbox() %>%
        jsonlite::toJSON(na = "null")
    )
    pb$tick()$print()

    if (httr::http_error(.request)) print(
      paste(
        "PATCH for record GUID", dplyr::pull(.cds_result, ent_primary_id_attr),
        "resulted in the following error:",
        httr::content(.request, encoding = "UTF-8")$error$message %>%
          stringr::str_sub(1, 300)
      )
    )

    .request
  }

  print(glue(
    "Updating {nrow(cds_df)} records in {md_ent$LogicalName} entity on ",
    "{token$name} ({token$web_api_url})."
  ))
  requests <-
    cds_df %>%
    parse_cds_enums(to = "key") %>%
    base::split(
      base::factor(.[[ent_primary_id_attr]], levels = .[[ent_primary_id_attr]])
    ) %>%
    purrr::map(.send_patch_request)

  requests_errors_n <- purrr::keep(requests, httr::http_error) %>% length()
  if (requests_errors_n > 0) warning(glue(
    "There were {requests_errors_n} requests with errors. Please review the ",
    "resulting list to troubleshoot."
  ))

  requests
}

#' Easily created CDS records from a tibble/ data frame.
#'
#' This function will send chained POST requests to target entity in order to
#' create new records with columns of a cds_df_like. For the moment this is a
#' simple function, does not perform any data validation, putting all strain
#' of validation checks on your CDS instance.
#'
#' @param cds_df_like tiible or data,frame with columns named as fields in the
#' target entity name and rows represeinting individula records to be created.
#' @param cds_instance_name A character scalar used in
#' \code{initialize_cds_token()} to name a CDS instance.
#' @param target_entity_set_name A plural name of the entity to which you want
#' to post new records.
#' @param exclude_null_cols A logical scalar indicating whether columns with
#' nulls should be included in a single record PATCH request. Especially useful
#' if you have null lookup field in your data for certain records and you want
#' to exclude just the to prevent the whole request from failing.
#'
#' @return List of httr requests objects for each record created. Useful to
#' investigate whether all requests went through and repeat for those who did
#' not.
#'
#' @export
create_cds_records <- function(cds_df_like,
                               cds_instance_name,
                               target_entity_set_name = NULL,
                               exclude_null_cols = FALSE) {

  force(cds_instance_name)
  token <- get_cds_token(cds_instance_name)
  pb <- dplyr::progress_estimated(nrow(cds_df_like))

  .send_post_request <- function(.single_cds_df_like) {
    .request <- httr::POST(
      paste0(token$web_api_url, target_entity_set_name),
      c(
        get_cds_headers(cds_instance_name),
        httr::add_headers(
          `MSCRM.SuppressDuplicateDetection` = "false",
          Prefer = "return=representation"
        )
      ),
      body = .single_cds_df_like %>%
        {
          if(exclude_null_cols) dplyr::select(., -where(~ all(is.na(.x))))
          else .
        } %>%
        jsonlite::unbox() %>%
        jsonlite::toJSON(na = "null")
    )
    pb$tick()$print()

    if (httr::http_error(.request)) print(
      paste(
        "POST request resulted in the following error:",
        httr::content(.request, encoding = "UTF-8")$error$message %>%
          stringr::str_sub(1, 300)
      )
    )

    .request
  }

  print(glue(
    "Creating {nrow(cds_df_like)} records in {target_entity_set_name} entity ",
    "on {token$name} ({token$web_api_url})."
  ))
  requests <-
    cds_df_like %>%
    base::split(1:base::nrow(.)) %>%
    purrr::map(.send_post_request)

  requests_errors_n <- purrr::keep(requests, httr::http_error) %>% length()
  if (requests_errors_n > 0) warning(glue(
    "There were {requests_errors_n} requests with errors. Please review the ",
    "resulting list to troubleshoot."
  ))

  requests
}

#   ____________________________________________________________________________
#   is_cds_df_valid_for_upload() to assert data sanity checks before manip  ####
is_cds_df_valid_for_upload <- function(cds_df, check_etags = TRUE) {
  md_ent <- get_cds_md(cds_df)
  ent_primary_id_attr <- dplyr::pull(md_ent, PrimaryIdAttribute)

  assert_that(
    ent_primary_id_attr %in% names(cds_df),
    msg = glue(
      "Entity's GUID column ({ent_primary_id_attr}) is not present in the ",
      "cds_df! Include it to update or delete records.")
  )
  assert_that(
    cds_df[[ent_primary_id_attr]] %>% identical(unique(.)),
    msg = glue(
      "The primary id attribute ({ent_primary_id_attr}) does not contain ",
      "unique values. Please deduplicate it and try again.")
  )

  if (check_etags) assert_that(
    "@odata.etag" %in% names(cds_df),
    msg = c(
      glue(
        "The {md_ent$LogicalName} entity does not have optimistic concurrency ",
        "enabled. Please force the update/upload by setting the ",
        "force_operation argument to TRUE"),
      paste(
        "@odata.etag column is not present in the cds_df! Please include it to",
        "ensure that you do not modify/delete records which changed after you",
        "downloaded them. ONLY IF YOU TRULY KNOW WHAT YOU ARE DOING, specify",
        "the force_operation argument to TRUE which ignores the etag checks.")
      )[dplyr::pull(md_ent, IsOptimisticConcurrencyEnabled) + 1]
  )

  TRUE
}
