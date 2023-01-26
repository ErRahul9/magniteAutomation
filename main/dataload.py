import psycopg2
import logging
import time
import os
import sys
import json
from pyformance import MetricsRegistry
# from wavefront_pyformance.wavefront_reporter import WavefrontDirectReporter
from psycopg2.extras import RealDictCursor
from redis.cluster import RedisCluster
from redis.cluster import ClusterNode
# from dotenv import load_dotenv
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
print(os.environ.get("REDIS_PORT"))


reg = MetricsRegistry()
rowCountGauge = reg.gauge("query.creativeId")
refresherTimer = reg.timer("refresh.processing")
# load_dotenv()



def reload_campaigns():
    process_reload()



def process_reload():
    try:
        logger.info("Start up redis connection")
        startup_nodes = [ClusterNode(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'])]
        rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)

        connection = psycopg2.connect(user=os.environ['POSTGRES_USER'],
                                      password=os.environ['POSTGRES_PW'],
                                      host=os.environ['POSTGRES_HOST'],
                                      port=os.environ['POSTGRES_PORT'],
                                      database=os.environ['POSTGRES_DATABASE'])

        logger.info('Reading data from core-db tables to load the cache.')
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            refresh_start_time = refresherTimer.time()
            cursor.execute("select c.campaign_id, c.advertiser_id, c.name, c.start_time, c.end_time, "
                           "c.campaign_group_id, c.cap, c.fcap_duration as fcap_duration, c.time_zone, c.currency, "
                           "c.company_url, c.campaign_status_id, c.deleted, c.pace_multiplier, "
                           "c.recency_threshold AS recency_threshold, c.objective_id, c.channel_id, "
                           "c.household_score_threshold, c.viewability_score_threshold, "
                           "adsettings.pmp_whitelist_eligible, dm.active as delivery_modifier_active, "
                           "dm.delivery_modifier_id,cs.pace_level from bidder.eligible_campaigns c "
                           "left join bidder.advertiser_settings adsettings ON c.advertiser_id = adsettings.advertiser_id "
                           "left join bidder.delivery_modifiers dm ON c.campaign_id = dm.campaign_id "
                           "left join bidder.campaign_settings cs ON c.advertiser_id = cs.advertiser_id and c.campaign_id = cs.campaign_id")
            records = cursor.fetchall()
            row_counter = 0
            for record in records:
                logger.debug("Record - {}".format(str(record)))
                delivery_modifier_id = record['delivery_modifier_id']
                delivery_modifier_mapping = {}
                creative_sizes = []
                if delivery_modifier_id:
                    with connection.cursor(cursor_factory=RealDictCursor) as term_cursor:
                        term_cursor.execute("select t.term_id, t.weight, t.active, t.rank_num from bidder.terms t "
                                            "where t.active = true and  t.delivery_modifier_id  = "
                                            + str(delivery_modifier_id))

                        term_records = term_cursor.fetchall()
                        terms = []
                        for term_record in term_records:

                            with connection.cursor(cursor_factory=RealDictCursor) as targeting_cursor:
                                targeting_cursor.execute("select t.target_id, t.comparator, t.expanded, t.target_value, "
                                                         "t.active, t.targeting_module  from bidder.targeting t "
                                                         #"join bidder.term_targets tt on t.target_id = tt.target_id "
                                                         "where t.active = true and t.term_id  = " + str(term_record['term_id']))

                                targeting_records = targeting_cursor.fetchall()
                                targets = []
                                for targeting_record in targeting_records:
                                    targets.append({
                                        "id": targeting_record['target_id'] or '',
                                        "comparator": targeting_record['comparator'] or '',
                                        "expanded": targeting_record['expanded'] or '',
                                        "value": targeting_record['target_value'] or '',
                                        "active": targeting_record['active'] or '',
                                        "targetingModule": targeting_record['targeting_module'] or ''
                                    })


                            terms.append({
                                "termId": term_record['term_id'] or '',
                                "weight": term_record['weight'] or '',
                                "rank": term_record['rank_num'] or '',
                                "active": term_record['active'] or '',
                                "targets": targets or []

                            })

                    delivery_modifier_mapping["modifierId"] = record['delivery_modifier_id'] or ''
                    delivery_modifier_mapping["active"] =record['delivery_modifier_active'] or ''
                    delivery_modifier_mapping["terms"] = terms

                campaignId = record['campaign_id'] or ''
                with connection.cursor(cursor_factory=RealDictCursor) as creative_cursor:
                    creative_cursor.execute("select width, height, creative_id, campaign_id, media_duration "
                                        "from bidder.ctv_creative_metadata "
                                        "where campaign_id = " + str(campaignId) + ";")

                    creative_records = creative_cursor.fetchall()

                    for creative_record in creative_records:
                        creative_sizes.append({
                            'creativeId': creative_record['creative_id'] or '',
                            'width': creative_record['width'] or '',
                            'height':  creative_record['height'] or '',
                            'mediaDurationSeconds': creative_record['media_duration'] or ''})


                threshold = {
                    "recencyThresholdMillis": record['recency_threshold'] or '',
                    "householdScoreThreshold": record['household_score_threshold'] or '',
                    "viewabilityScoreThreshold": record['viewability_score_threshold'] or ''
                }
                # Records are addressable via a tuple of (namespace, set, key)
                mapping = {
                    "advertiserId": record['advertiser_id'] or '',
                    "name": record['name'] or '',
                    "startTime": record['start_time'] or '',
                    "endTime": record['end_time'] or '',
                    "campGrpId": record['campaign_group_id'] or '',
                    "objectiveId": record['objective_id'] or '',
                    "channelId": record['channel_id'] or '',
                    "creativeMetadata": creative_sizes,
                    "fcap": record['cap'] or '',
                    "fcapDuration": record['fcap_duration'] or '',
                    "currency": record['currency'] or '',
                    "companyUrl": record['company_url'] or '',
                    "campStatusId": record['campaign_status_id'] or '',
                    "deleted": record['deleted'] or '',
                    "campaignId": record['campaign_id'] or '',
                    "paceMultiplier": record['pace_multiplier'] or '',
                    "threshold": threshold,
                    "deliveryModifier": delivery_modifier_mapping,
                    "pace": record['pace_level'] or '',
                    "timezone": record['time_zone'] or '',
                    "advertiserPmpWhiteListEligible": record['pmp_whitelist_eligible'] or False

                }

                try:
                    # Write a record
                    payload = json.dumps(mapping, sort_keys=True, default=str)
                    key = record['campaign_id']
                    rc.set(key, payload)


                    print(payload)
                except Exception as e:
                    print("error: {0}".format(e), file=sys.stderr)

                row_counter += 1
            rowCountGauge.set_value(row_counter)
            refresherTimer.time(refresh_start_time.stop())
            logger.info('Completed loading data from mountain campaigns for magnite campaignByCampaignId cache.')
    except (Exception, psycopg2.Error) as error:
        logger.error("Error while connecting to PostgresSQL", error)
        # wf_direct_reporter.stop()
        raise error


if __name__ == "__main__":
    reload_campaigns()