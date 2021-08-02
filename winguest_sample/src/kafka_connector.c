#include "kafka_connector.h"
#include <librdkafka/rdkafka.h>

static char *brokers = "dory-01.srvs.cloudkafka.com:9094,dory-02.srvs.cloudkafka.com:9094,dory-03.srvs.cloudkafka.com:9094";
static char *topic = "ni79x7pe-processes";

static     rd_kafka_t *rk;         /* Producer instance handle */

int KafkaConnectorInit(void)
{
    rd_kafka_conf_t *conf;  /* Temporary configuration object */
    char errstr[512];       /* librdkafka API error reporting buffer */


    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
        errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "security.protocol", "SASL_SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "sasl.mechanisms", "SCRAM-SHA-256", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "sasl.username", "ni79x7pe", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "sasl.password", "M4atY10pWTPB62yfh61MSshi9j87mj5E", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    /*
     * Create producer instance.
     *
     * NOTE: rd_kafka_new() takes ownership of the conf object
     *       and the application must not reference it again after
     *       this call.
     */
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
    {
        fprintf(stderr,
            "%% Failed to create new producer: %s\n", errstr);
        return 1;
    }

    return 0;
}

int KafkaConnectorSentMessage(char *Message, unsigned int SizeOfMessage)
{
    rd_kafka_resp_err_t err;

retry:
    err = rd_kafka_producev(
        /* Producer handle */
        rk,
        /* Topic name */
        RD_KAFKA_V_TOPIC(topic),
        /* Make a copy of the payload. */
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        /* Message value and length */
        RD_KAFKA_V_VALUE(Message, SizeOfMessage),
        /* Per-Message opaque, provided in
         * delivery report callback as
         * msg_opaque. */
        RD_KAFKA_V_OPAQUE(NULL),
        /* End sentinel */
        RD_KAFKA_V_END);
    if (err)
    {
        /*
         * Failed to *enqueue* message for producing.
         */
        fprintf(stderr,
            "%% Failed to produce to topic %s: %s\n",
            topic, rd_kafka_err2str(err));
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
        {
            /* If the internal queue is full, wait for
             * messages to be delivered and then retry.
             * The internal queue represents both
             * messages to be sent and messages that have
             * been sent or failed, awaiting their
             * delivery report callback to be called.
             *
             * The internal queue is limited by the
             * configuration property
             * queue.buffering.max.messages */
            rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
            goto retry;
        }
    }
    else {
        fprintf(stdout, "%% Enqueued message (%zd bytes) "
            "for topic %s\n",
            SizeOfMessage, topic);
    }

    return 0;
}

int KafkaConnectorUninit(void)
{
    /* Wait for final messages to be delivered or fail.
     * rd_kafka_flush() is an abstraction over rd_kafka_poll() which
     * waits for all messages to be delivered. */
    fprintf(stderr, "%% Flushing final messages..\n");
    rd_kafka_flush(rk, 10 * 1000 /* wait for max 10 seconds */);
    /* If the output queue is still not empty there is an issue
     * with producing messages to the clusters. */
    if (rd_kafka_outq_len(rk) > 0)
        fprintf(stderr, "%% %d message(s) were not delivered\n",
            rd_kafka_outq_len(rk));
    /* Destroy the producer instance */
    rd_kafka_destroy(rk);

    return 0;
}
