#ifndef _KAFKA_CONNECTOR_
#define _KAFKA_CONNECTOR_

int KafkaConnectorInit(void);
int KafkaConnectorUninit(void);
int KafkaConnectorSentMessage(char *Message, unsigned int SizeOfMessage);

#endif // !_KAFKA_CONNECTOR_
