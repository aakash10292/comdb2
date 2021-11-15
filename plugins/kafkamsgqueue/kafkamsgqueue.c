#include "comdb2_plugin.h"
#include "comdb2_message_queue.h"
#include "intern_strings.h"
#include "logmsg.h"
#include "str0.h"
#include "librdkafka/rdkafka.h"

static rd_kafka_topic_t *rkt_p = NULL;
static rd_kafka_t *rk_p = NULL;

const char *gbl_kafka_brokers_physrep = "localhost:9029";
const char *gbl_kafka_topic_physrep = "aartopic";

int kafka_publish(void *data, int data_len) {
    rd_kafka_conf_t *conf;  /* Temporary configuration object */
    char errstr[512];       /* librdkafka API error reporting buffer */

    if (!gbl_kafka_topic_physrep || !gbl_kafka_brokers_physrep) {
        logmsg(LOGMSG_USER, "%s: Kafka Topic or Broker not set!\n", __func__);
        return -1;
    }

    if (!rk_p) {
        conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(conf, "bootstrap.servers", gbl_kafka_brokers_physrep,
                             errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            logmsg(LOGMSG_USER, "%s: Error setting kafka brokers!"
                    "Kafka Error String : %s\n", __func__, errstr);
            return -1;
        }

        rk_p = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

        if (!rk_p) {
            logmsg(LOGMSG_USER, "%s: Error creating kafka producer!"
                    "Kafka Error String : %s\n", __func__, errstr);
            return -1;
        }

    }
    if (!rkt_p)  {
        rkt_p = rd_kafka_topic_new(rk_p, gbl_kafka_topic_physrep, NULL);
        if (!rkt_p) {
            logmsg(LOGMSG_USER, "%s: Error creating kafka topic object!"
                    "Kafka Error String : %s\n", __func__, rd_kafka_err2str(rd_kafka_last_error()));
            return -1;
        }
    }

    if (rd_kafka_produce(
            /* Topic object */
            rkt_p,
            /* Use builtin partitioner to select partition*/
            RD_KAFKA_PARTITION_UA,
            /* Make a copy of the payload. */
            RD_KAFKA_MSG_F_COPY,
            /* Message payload (value) and length */
            (void*)data, data_len,
            /* Optional key and its length */
            NULL, 0,
            /* Message opaque, provided in
             * delivery report callback as
             * msg_opaque. */
            NULL) == -1) {
        /**
         * Failed to *enqueue* message for producing.
         */
            logmsg(LOGMSG_USER,"%% Error publishing to kafka topic %s."
                    "Kafka Error String: %s\n", rd_kafka_topic_name(rkt_p), rd_kafka_err2str(rd_kafka_last_error()));
            return -1;
    }
    return 0;
}
static void kafka_subscribe(){}
static int kafka_init(void *unused)
{
    return 0;
}

comdb2_queue_pub_t kafka_publisher_plugin = {
    .type = 0,
    .publish = kafka_publish,
};

comdb2_queue_sub_t kafka_subscriber_plugin = {
    .type = 0,
    .subscribe = kafka_subscribe,
};

#include "plugin.h"
