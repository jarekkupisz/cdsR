
<!-- README.md is generated from README.Rmd. Please edit that file -->

# cdsR - R Interface to Microsoft’s Dataverse (formerly Common Data Service - CDS)

Dataverse (formerly Common Data Service - CDS) is Microsoft’s unified
data storage for all business applications marketed under the Power Apps
umbrella. As many companies are using Microsoft’s technology stack,
Dataverse is an important data source for analysts and data scientists.
The goal of this package is to make it dead simple to get your data from
(and to) your Dataverse Web API instance and leverage its rich metadata
to automate data analysis tasks. The package offers a suite of pipeable,
simple to use functions shielding the user from the unmemorizable syntax
of OData requests and ensuring correct conversion of data types (looking
at you picklists!).

## Installation and Usage Disclaimers

To install simply run:

``` r
devtools::install_github("jarekkupisz/cdsR")
```

As you may notice the package is not on CRAN, there are no runnable
examples and no tests folder. The reason is that for the package to work
you need to initialize a connection to the [Dataverse’s Web
API](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/webapi/overview).
This is not currently possible outside your organization’s Microsoft
Power App’s tenant and without creating an application user with unique
and not sherable credentials.

Despite this, the package is fully featured, documented, and is ready
for production-grade tasks. I am not affiliated with Microsoft in any
way and this is not an official API by any means! However, as I use it
daily in my work, I thought that other \#rstats souls working with MS
stack might find it very useful.

Why `cdsR`? As the package was created by me months before MS decided to
rebrand Common Data Service, I just stayed with the old name. What is
more, Dataverse just has no sensible abbreviation which would work
nicely with the package’s naming convention.

## Connecting to your Dataverse

If you want to get your data and use other functions in the package,
first you need to authenticate. Simply run:

``` r
library(cdsR)
initialize_cds_token(
  cds_instance_name = "prod", #choose any name you like for a Dataverse instance
  cds_instance_url = "https://yourorg.crm3.dynamics.com",
  tenant_id = "your tenant guid",
  client_id = "your user guid",
  client_secret = "your user pass",
  web_api_version = "9.1" #choose any you like, 9.1 is the default for now
)
```

This function will create a `cds_token` S3 object in the `options()` of
your R session. You can set as many tokens as you like, as long as you
give each a unique `cds_instance_name`. I typically create one for
“prod”, “testing” and “dev” instances. Get all initialized tokens with
`getOption("cdsR.tokens")`.

They also automatically refresh! You don’t need to keep calling
`initialize_cds_token()`. Just do it once at the start of your work or
in `.Rprofile`. Before each function call the token checks how much time
passed before it got its credentials, and if too much, it will get fresh
ones!

## Download Data from Your Dataverse as a tibble

Ready to get some data? If you know your entity’s logical plural name
then:

``` r
download_cds("contacts")
```

Wait, that’s it? The short answer is yes! If you want more features
though follow along.

First, how come `download_cds()` knows where to connect? If there is
only one cdsR token set it will use it automatically. That’s just so
convenient in interactive sessions where you toy with your data. But
what if you have scripts that need to run on multiple Dataverse
instances or you want to write a readable, production-ready code? That’s
where the `cds_instance_name` argument comes in. A `cds_token` object
from `options()` with the specified name will be selected:

``` r
download_cds("contacts", cds_instance_name = "prod")
```

The pattern below is something I use all the time if I want to download
the same entity from multiple instances:

``` r
c("prod", "testing") %>% purrr::map(~ download_cds("contacts", .x))
```

Okay then but this query will simply pull all possible columns and
records but typically that’s not what we want. We want either a subset
of columns, apply a specific filter or just to download a few rows to
play with the data. Hopefully, you want one thing at a time, because
writing your own OData HTTP requests is one of those moments that make
you hate your job. Fortunately, other arguments of `download_cds()` come
to the rescue:

``` r
download_cds(
  "contacts",
  select = c("fullname", "pa_salesscore", "_owninguser_value"),
  filter = cds_filter_fun("LastXMonths", "createdon", PropertyValue = 3),
  order_by = "pa_salesscore desc, fullname",
  top = 100
)
```

Woah a complex OData query using only understandable R expressions!
There’s a couple of things to unpack, so let’s dive in:

-   Each of the presented arguments represents respective OData system
    operators: `$select`, `$filter`, `$order_by` and `$top`. You can
    pass any character vector that would be a valid part of an OData
    operand. If such vector had a longer length than 1, it would be
    collapsed to an OData expression (see above `select`). In the case
    of `filter` the expressions will be pasted with `and` similarly to
    dplyr’s `filter`. `top` of course requires a single integer.
-   The toughest part of these requests are [filtering
    functions](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/queryfunctions?view=dynamics-ce-odata-9).
    `cdsR` provides a helper `cds_filter_fun()` that takes typically 2
    strings – a Web API filter function name and a field on which you
    wish to subset. For certain conditions you’ll need more than 2, then
    pass named arguments as the `…`.
-   The function will monitor its progress after each page of the
    results is downloaded (every 5k rows). Unfortunately, there is no
    quick and reliable way of getting the count of records for bigger
    entities, that’s why a percentage of completion estimate is off the
    table.

## `cds_df` – A Bridge Between Dataverse and R

So the result that you get from `download_cds()` loos like a `tibble`
but when you investigate closely you can see some pleasant surprises.
This all comes down to the fact that the package strips off the
weirdness of some of the OData data types and converts everything into
human and R readable format:

-   All enum type fields such as picklists, state, status fields are
    converted to character vectors representing option set labels.
-   Lookup fields get special treatment. Each of them is represented as
    two columns: one with an original name containing `GUID`s character
    vectors and one with a display attribute of the target entity
    suffixed with `.display`. If you want to know which user is a
    record’s owner look for an `owninguser` column to get their GUID and
    `owninguser.display` for their name.
-   Multiselect picklists are represented as a list of character
    vectors. Each entry in a vector represents a choice made in a field
    for a row.
-   All date and datetime fields are converted to POSIX vector’s using
    `lubridate` magic. Remember that the Dataverse works only in UTC!
-   Boolean flags are converted to logical vectors.
-   Nulls are converted to `NA`s.

### `cds_ds`’s Dataverse Metadata

Apart from sensible type conversions that allow you to easily use the
data, there is more utility hiding in data frames returned by
`download_cds()`. Actually, I lied to you as technically what you get is
a `cds_df` object, which is a tibble with some addons. In the attributes
of `cds_df`s’ you’ll find 3 entries that contain the whole [Dataverse
metadata](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/webapi/use-web-api-metadata)
of the entity you just downloaded!

There is a helper function `get_cds_md()` which allows you to specify
which definition you wish to extract with `which_md` argument:

-   `”entity”` the default, represents a tibble (an actual one this
    time!) with MD about the entity itself
-   `”attributes”` gets you a tibble with MD containing all information
    about entity’s fields
-   `”picklists”` returns a list of tibbles for each enum/picklist type
    field in the `cds_df` holding pair of keys and labels for each
    option

Let’s give this rich metadata a spin. Let’s assume you forgot what is
the display name for the entity you are working with:

``` r
contacts <- download_cds("contacts")
contacts %>% get_cds_md() %>% dplyr::select(DisplayName)
```

This time you want to select all picklists for your model to run on:

``` r
contacts %>%
  select(
    get_cds_md(., "attributes") %>% 
      filter(AttributeType == "Picklist") %>% 
      pull(LogicalName)
  )
```

As long as the info you need hides somewhere in the Dataverse’s metadata
you can use it in your code.

## Update Records in your Dataverse

Manipulating data in your session is fun for our analytical minds, but
the most potent way we can contribute to our organizations is to return
the results of our numerical work to those who will use it. As all Power
Apps work on Dataverse, updating records in your instance will benefit
all of your business applications at once! I hate the word, but this is
a powerful concept from Microsoft.

You’ve just trained a customer churn prediction algorithm and want to
help your sales reps by sorting their contact list by a propensity to go
away. `update_cds_records` is your buddy this time. We’ll use `contacts`
entity we downloaded earlier:

``` r
contacts %>%
  inner_join(churn_algo_results, by = "contactid") %>%
  select(contactid, ap_churnrisk = prob_to_churn) %>%
  update_cds_records(
    cds_instance_name = "prod",
    fail_safe_entity_name = "contact",
    force_operation = TRUE
  )
```

This call will even give you an ETA for the end of the update job and
warn about any errors no the way. This function though works a bit
differently:

-   `cds_instance_name` has to be always specified this time. Because we
    are launching potentially a destructive operation (and anyone who
    tried to recover data from Dataverse gets MS hell at its “finest”)
    the package needs to make sure you know what you are doing.
-   `fail_safe_entity_name` serves basically as another defence to save
    you from dire consequences. This function is easy to call at the end
    of the pipe and I got used to providing the first argument too
    quickly. This argument forces you to explicitly type which Dataverse
    table you want to update and the function will confirm with
    `cds_df`’s metadata that indeed you are good to go.
-   `force_operation` is yet another fail-safe. By default, it is
    `FALSE` and it uses [Dataverse Web API optimistic concurrency
    control](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/webapi/perform-conditional-operations-using-web-api#bkmk_limi)
    to prevent you from updating records that might have been changed
    before you downloaded them. If you don’t wish to overwrite any
    changes you’ll need to include the `@odata.etag` column which is
    included in every `download_cds()` result.

### Automatic Type Conversion with `update_cds_records()`

Ok then, simple enough for small requests like the one above. What about
`state` column? Picklists, multipicklists, dates? They all need to be
parsed to integer keys or specially formatted strings for the Web API to
accept a POST call, right? Well `update_cds_recrods()` does everything
for you! Simply use the human-readable labels that were returned to you
with `download_cds()` and pass the mutated and filtered `cds_df` you are
working with to `update_cds_records()`! There are a few quirks though:

-   Lookups don’t work that nicely. This is something I will try to
    improve, but the only way to change the value in the lookup is to
    create a column named `lookup_relationship_name@odata.bind` and have
    a character vector with the following template for values:
    `target_entity_plural_name(GUID _of_the_target_record)`. An example
    would be

``` r
contacts %>% 
  mutate(
    `ownerid@odata.bind`="systemusers(2zcaa784-0dd5-dr11-81e4-123c2981575g)"
  )
```

-   When launching the `update_cds_records()` a provided data frame has
    to be a `cds_df` with all the metadata in its `attributes()`,
    otherwise, the function errors. `dplyr`’s verbs, unfortunately, get
    rid of attributes. That’s why `cdsR` registers S3 generic methods
    for `mutate`, `*_join`, `select` and all else that was possible to
    register to preserve attributes. In my daily work I rarely stumble
    onto this problem, but if you do then you can use `is.cds_df()` to
    troubleshoot if you are still dealing with a `cds_df`.
-   If you wish to work with integer keys instead of character labels
    for enum fields for any reason (I have not found a good one yet, so
    please let me know when you do!) you can leverage
    `parse_cds_enums(cds_df, to = "key")`

## Deleting Records

Cleaning time! Duplicates and legacy records are a common plague in any
CRM-like application. As a data scientist, you’ve just trained an ML
model that recognizes low utility entries. Just pipe away:

``` r
contacts %>%
  filter(contactid %in% cleaning_algo_results$junk_contacts_guids) %>%
  delete_cds_records("prod", "contact")
```

All the fail-safe features are the same as in the
`update_cds_recrods()`. For example, the above code would not remove any
rows that were changed before you used `download_cds()` to get your
contacts. Please refer to the function’s documentation or previous
section for the details.

## Creating New Records

This seems like the least useful operation, however, if you worked with
any CRM-type system in the past you know that some kind of data
migration/integration awaits you. That’s why I decided to include a
simple function `create_cds_recrods()`. It allows you to create new
records from a fresh data frame. This function does not include any of
the fancy type conversions of `update_cds_records()`. I suggest
leveraging `get_cds_md()` on a `cds_df` representing a Dataverse table
you wish to produce records in.

``` r
new_contacts <- tribble(
  ~firstname, ~lastname, ~department,
  "Andy",     "Doe",      1,
  "John",     "Smith",    3 
)
create_cds_records(
  new_contacts,
  cds_instance_name = "prod",
  target_entity_set_name = "contacts"
)
```
