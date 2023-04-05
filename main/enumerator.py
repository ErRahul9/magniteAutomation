import enum


class bid(enum.Enum):
    ip = "ip"
    companyURL = "bundle"
    height = "h"
    duration ="maxduration"
    width ="w"
    linear = "linearity"
    skip="skip"
    deviceType="devicetype"

class database(enum.Enum):
    campaign_id = "campaign_id"
    advertiser_id = "advertiser_id"
    name = "name"
    campGrpId = "campaign_group_id"
    objective_id ="objective_id"
    channel_id = "channel_id"
    cap="cap"
    fcapDuration = "fcap_duration"
    timezone="time_zone"
    currency="currency"
    companyURL="company_url"
    campStatusId="campaign_status_id"
    deleted="deleted"
    pace_multiplier="pace_multiplier"
    startTime= "start_time"
    endTime="end_time"
    width = "width"
    height = "height"
    creative_id="creative_id"
    duration="media_duration"
    active="active"
    recency_threshold="recency_threshold"
    household_score_threshold="household_score_threshold"
    viewability_score_threshold="viewability_score_threshold"
    pmp_whitelist_eligible="pmp_whitelist_eligible"
    delivery_modifiers="delivery_modifier_id"
    pace="pace_level"
















