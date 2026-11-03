# Feature: Retention


<!-- toc -->

- [1. Feature Context](#1-feature-context)
  - [1.1 Overview](#11-overview)
  - [1.2 Purpose](#12-purpose)
  - [1.3 Actors](#13-actors)
  - [1.4 References](#14-references)
- [2. Actor Flows (CDSL)](#2-actor-flows-cdsl)
- [3. Processes / Business Logic (CDSL)](#3-processes--business-logic-cdsl)
  - [The Tenant's Retention Period](#the-tenants-retention-period)
  - [Remove What Is Past the Period](#remove-what-is-past-the-period)
  - [Hide What Is Past the Period](#hide-what-is-past-the-period)
- [4. States (CDSL)](#4-states-cdsl)
- [5. Definitions of Done](#5-definitions-of-done)
  - [The Retention Period](#the-retention-period)
  - [The Retention Job](#the-retention-job)
  - [Nothing Served Past the Period](#nothing-served-past-the-period)
- [6. Acceptance Criteria](#6-acceptance-criteria)
- [7. Non-Functional Considerations](#7-non-functional-considerations)

<!-- /toc -->

- [ ] `p2` - **ID**: `cpt-cf-construct-featstatus-retention-implemented`

<!-- reference to DECOMPOSITION entry -->
- [ ] `p2` - `cpt-cf-construct-feature-retention`

## 1. Feature Context

### 1.1 Overview

Retention removes what is older than the tenant's retention period: facts, counted from when they were stored, review requests from when they were created, and record identities from when they were received. A job on the cluster leader runs it with the same delete code as erasure (`cpt-cf-construct-feature-erasure`), and the Profile Reader stops serving a fact once it is past the period, before the job removes it. This document covers the retention part of entry 2.15; tenant exit comes later.

### 1.2 Purpose

The PRD requires that data is not kept longer than the tenant's retention period, which is a tenant setting; that without one, data stays until it is deleted; and that audit events follow the Audit gear's retention period instead (`cpt-cf-construct-fr-retention`). It also requires that deleted data is not served from the next read (`cpt-cf-construct-nfr-deletion-time`), and that the period ends the keeping of record identities (`cpt-cf-construct-fr-subject-delete`).

**The period.** The retention period is a tenant setting of the settings service. No Construct setting is declared there yet, so the deployment's config gives it, behind the same interface the settings service will implement: `retention.tenants` gives a tenant its own period in days, by tenant id, and `retention.default_days` gives every other tenant one. Without either, a tenant keeps its data until it is deleted; that is the default. A period is from 1 to 36,500 days.

**The job.** The cluster leader's deletion job (`cpt-cf-construct-algo-erasure-rerun`) runs retention every `deletion.job_interval_secs` (300), in a loop of its own beside the reruns of stopped erasures, so a long sweep does not hold them off. It sweeps each tenant listed in `retention.tenants` and, with a default period, every tenant that holds a row in Construct's tables or ever held one: the job keeps those tenants in `construct__tenants`, so a tenant stays in the sweep after retention removed its rows and while its facts in graph storage remain. A period that reaches before the earliest time there is removes nothing. For each tenant, as Construct's processing identity, it:

1. Removes the review requests created before the period began, open and closed, and the record identities received before it.
2. Lists the subjects that have a profile, through the profile roots in graph storage, a page at a time, and for each removes the facts stored before the period began, as one plan of removals with retention as origin, written against the profile's version and read and written again on a conflict within the rerun limit and the write budget. A fact that a write replaced meanwhile is stored anew, so it is decided again and kept. Then every open review request of the subject whose fact is gone closes as deleted, closed by Construct's identity: those of the facts removed now, and any an earlier run removed but could not close. Every close is tried; a subject that fails is logged and left for the next run, which finds what is left.

A record identity removed by retention no longer makes a resend of that record a repeat. A resend more than a period later is taken as a new record.

**The read.** The Profile Reader does not serve a fact stored before the tenant's period began, also before the job removes it (`cpt-cf-construct-algo-profile-reader-read`). The subject's own view, the export and an administrator's read still show it until the job removes it. The planner's read of the profile leaves such a fact out too, so no plan is built on an expired value.

**Known gaps.**

- Removal is graph storage's soft delete until it ships a hard delete (gears-rust #5194), as for erasure.
- A tenant whose only data is facts an administrator added, without a record, a review request or a settings row, is swept only when it is listed in `retention.tenants`.
- Without the cluster profile `construct`, the job does not run, and nothing is removed for retention; the read still hides what is past the period.
- Retention removes the record identities past the period, so a record sent again after that, or one sent late with an old observation time, is not a repeat. Record Intake does not compare a record's observation time with the tenant's period, so its facts are stored again with a fresh stored time and kept for a whole period. A connector **SHOULD NOT** send again a record older than the tenant's retention period.

**Out of scope**:

- Tenant exit, which follows the platform's tenant offboarding protocol and waits on graph storage's offboarding.
- Setting the period in the settings service, and declaring Construct's settings there.
- Audit events, which the Audit gear's retention period removes.

**Requirements**: `cpt-cf-construct-fr-retention`; shared: `cpt-cf-construct-nfr-deletion-time` (nothing served past the period), `cpt-cf-construct-fr-subject-delete` (the period ends the keeping of record identities).

**Principles**: None. **Constraints**: None.

### 1.3 Actors

| Actor | Role in Feature |
|-------|-----------------|
| `cpt-cf-construct-actor-tenant-admin` | Sets the retention period, in the deployment's config until the settings service holds it. |
| `cpt-cf-construct-actor-graph-storage` | Lists the profile roots and holds the facts that are soft-deleted. |
| `cpt-cf-construct-actor-audit` | Receives the counts of what was removed, through the structured log until the Audit gear ships. |

### 1.4 References

- **PRD**: [PRD.md](../PRD.md)
- **Design**: [DESIGN.md](../DESIGN.md), the Deletion component
- **Decomposition**: [DECOMPOSITION.md](../DECOMPOSITION.md), entry 2.15
- **Dependencies**: `cpt-cf-construct-feature-erasure`, `cpt-cf-construct-feature-profile-reader`

## 2. Actor Flows (CDSL)

None. No actor calls retention: the leader's job runs it.

## 3. Processes / Business Logic (CDSL)

### The Tenant's Retention Period

- [x] `p2` - **ID**: `cpt-cf-construct-algo-retention-period`

**Input**: The tenant id.

**Output**: The period, or none.

**Steps**:
1. [x] - `p2` - **RETURN** the tenant's own period from `retention.tenants`, else `retention.default_days`, else none, as days - `inst-period-read`

### Remove What Is Past the Period

- [x] `p2` - **ID**: `cpt-cf-construct-algo-retention-expire`

**Input**: The tenant id and its period.

**Steps**:
1. [x] - `p2` - Sweep each tenant in `retention.tenants` and, with a default period, each tenant that holds or held a row in Construct's tables, recording a new one in `construct__tenants`; read its period, and skip a tenant without one or one whose list or expiry fails, logged - `inst-expire-tenants`
2. [x] - `p2` - **IF** the period reaches before the earliest time there is, nothing. Build Construct's context and scope in the tenant. DB: delete the tenant's review requests created before the period began and its record identities received before it, 1000 at a time; record the counts when any was removed - `inst-expire-rows`
3. [x] - `p2` - Graph storage: list the tenant's profile roots a page of 100 at a time, each naming its subject, until no next page; a root whose payload cannot be read, or whose key is not that subject's root in the tenant, is left out and logged as an error. **IF** graph storage names the page just read as the next, the tenant's run stops with an error; **IF** 10000 pages were read, the listing stops there and says so in an error - `inst-expire-subjects`
4. [x] - `p2` - For each subject, remove the facts stored before the period began as one plan of removals with retention as origin, written against the profile's version, read and written again after a conflict within the rerun limit and the write budget; record the count when any was removed. A subject that fails is logged - `inst-expire-facts`
5. [x] - `p2` - Close as deleted every open review request of the subject whose fact is not among its live facts, closed by Construct's identity, as `cpt-cf-construct-dod-review-queue-subject-control` says: a reviewer's resolution recorded on a request whose fact retention did not remove, and whose write is in the profile, closes it as that resolution; try every one, and log each failure with its fact; a subject with a failure counts as one with requests left open, not as one past its period - `inst-expire-close`

### Hide What Is Past the Period

- [x] `p2` - **ID**: `cpt-cf-construct-algo-retention-hide`

**Steps**:
1. [x] - `p2` - In the profile read and the planner's read, after the facts under review are left out, leave out every fact stored before the tenant's period began; a period reaching before the earliest time there is hides nothing - `inst-hide-expired`

## 4. States (CDSL)

None.

## 5. Definitions of Done

### The Retention Period

- [x] `p2` - **ID**: `cpt-cf-construct-dod-retention-period`

The system **MUST** take a tenant's retention period from the deployment's config, the tenant's own before the default, and **MUST** keep a tenant's data until it is deleted when neither gives one. A period out of range **MUST** stop the gear from starting.

**Implements**:
- `cpt-cf-construct-algo-retention-period`

**Touches**:
- Config: `retention.default_days`, `retention.tenants`

**Verified by**: `a_listed_tenant_has_its_own_period_and_every_other_the_default_or_none` (periods), `retention_keeps_data_by_default_and_the_job_runs_every_five_minutes`, `the_retention_and_deletion_keys_are_read`, `a_retention_or_deletion_value_out_of_range_is_refused` (config).

### The Retention Job

- [x] `p2` - **ID**: `cpt-cf-construct-dod-retention-job`

The system **MUST** remove, on the cluster leader, each tenant's review requests, record identities and facts that are older than its period, with the same delete code as erasure, so a fact a write changed meanwhile is decided again, and **MUST** close an open review request on a removed fact as deleted. A tenant without a period **MUST NOT** be swept.

**Implements**:
- `cpt-cf-construct-algo-retention-expire`

**Touches**:
- Graph storage: the profile roots (listed), the facts (soft-deleted), the root's version
- DB Tables: `construct__review_requests`, `construct__record_ids`
- Config: `deletion.job_interval_secs`

**Verified by**: `retention_removes_only_the_facts_past_the_period_and_closes_their_open_requests`, `retention_with_nothing_past_the_period_writes_nothing`, `retention_walks_every_page_of_subjects`, `retention_whose_write_conflicts_decides_again_on_the_new_profile`, `retention_records_the_rows_it_removed_and_nothing_when_there_were_none`, `a_request_left_open_on_a_fact_an_earlier_run_removed_is_closed_by_the_next`, `a_subject_whose_requests_cannot_be_closed_does_not_stop_the_others`, `every_close_is_tried_when_one_fails_and_the_tenant_reports_the_request_left_open`, `a_period_reaching_before_the_earliest_time_removes_nothing`, `a_listing_that_hands_back_the_same_page_stops_retention_of_the_tenant`, `a_listing_that_never_ends_stops_at_the_page_limit_and_says_so` (Deletion), `the_job_sweeps_the_listed_tenants_and_with_a_default_every_tenant_held`, `without_a_period_nothing_is_swept`, `a_tenant_that_fails_does_not_stop_the_others`, `without_the_held_tenants_only_the_listed_ones_are_swept`, `a_retention_sweep_that_does_not_end_does_not_hold_off_a_stopped_erasure` (job), `retention_removes_the_tenants_rows_from_before_the_cut_and_no_other`, `retention_removes_more_identities_than_one_batch_holds_batch_by_batch`, `the_tenants_are_those_with_a_row_in_any_table`, `a_tenant_stays_known_after_retention_removed_its_rows` (store), `the_subjects_with_a_root_are_listed_only_in_the_tenant`, `a_cursor_that_cannot_be_read_is_a_defect`, `only_a_root_key_of_the_subject_in_the_tenant_is_its_root`, `a_root_names_its_subject_and_an_unreadable_one_says_why` (the root listing, on graph storage's in-memory service, which answers one page), `the_directory_hands_on_graph_storages_cursor_and_leaves_out_a_root_it_cannot_read` (the root listing over a scripted page with a next cursor and two bad roots).

### Nothing Served Past the Period

- [x] `p2` - **ID**: `cpt-cf-construct-dod-retention-hide`

The Profile Reader **MUST NOT** serve, and the planner **MUST NOT** read, a fact stored before the tenant's retention period began, also before the job removes it.

**Implements**:
- `cpt-cf-construct-algo-retention-hide`

**Touches**:
- The profile read

**Verified by**: `a_fact_stored_before_the_tenants_retention_period_is_not_served`, `a_period_reaching_before_the_earliest_time_hides_nothing` (profile read), `the_planner_reads_no_fact_past_the_retention_period_while_the_subject_still_sees_it` (planner's read).

## 6. Acceptance Criteria

- [ ] A fact past the retention period is not served, and the job removes it (`a_fact_stored_before_the_tenants_retention_period_is_not_served`, `retention_removes_only_the_facts_past_the_period_and_closes_their_open_requests`).
- [ ] Review requests and record identities past the period are removed, in that tenant only (`retention_removes_the_tenants_rows_from_before_the_cut_and_no_other`).
- [ ] Without a period, nothing is removed (`without_a_period_nothing_is_swept`).
- [ ] The tests named in this document pass.

## 7. Non-Functional Considerations

- **Security**: The job runs as Construct's processing identity in each tenant, and its rows are removed under that tenant's scope. No caller is involved.
- **Privacy**: Data older than the period is not served from the next read and is removed by the next run. No event holds a value; an event gives counts.
- **Data integrity**: Facts are removed as one plan against the profile's version, so a fact a write replaced meanwhile is kept. Rows are removed by their own time, so a request on a kept fact goes only when the request itself is past the period.
- **Reliability**: What fails is logged and left for the next run; a subject that fails does not stop the tenant's others. A listing that repeats its page stops the tenant's run, and one that never ends stops at 10000 pages.
- **Performance**: Each run reads every profile of each swept tenant, one at a time, so a run takes longer as tenants grow; the interval bounds how often.
- **Observability**: Events go to `construct::audit`; a failed tenant or subject is a warning, and a tenant that left any subject past its period ends with one error saying how many of how many, and one where only review requests stayed open with one warning.
- **Not done in this part**: see the out-of-scope list and the known gaps in 1.2.
