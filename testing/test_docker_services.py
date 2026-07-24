import json
import logging
import time
import socket
import subprocess
import sys
from asyncio import timeout
from http.client import responses
from typing import Dict, Any, List, Optional
from datetime import datetime
import requests
from confluent_kafka import Producer, Consumer,KafkaError,KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import pyspark
from pyspark.sql import SparkSession
import pytest

#configure logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DockerServiceTesters:

    """
    Test all docker compose services
    """

    def __init__ (self):
        self.services={
            'kafka1': {'host': '127.0.0.1', 'port': 9092, 'name': 'Kafka Broker 1'},
            'kafka2': {'host': '127.0.0.1', 'port': 9093, 'name': 'Kafka Broker 2'},
            'kafka3': {'host': '127.0.0.1', 'port': 9094, 'name': 'Kafka Broker 3'},
            'kafka4': {'host': '127.0.0.1', 'port': 9095, 'name': 'Kafka Broker 4'},
            'kafdrop': {'host': '127.0.0.1', 'port': 9000, 'name': 'Kafdrop UI'},
            'spark-master': {'host': '127.0.0.1', 'port': 9090, 'name': 'Spark Master UI'},
            'spark-worker-1': {'host': '127.0.0.1', 'port': 8090, 'name': 'Spark Worker 1'},
            'spark-worker-2': {'host': '127.0.0.1', 'port': 8091, 'name': 'Spark Worker 2'},
            'spark-worker-3': {'host': '127.0.0.1', 'port': 8092, 'name': 'Spark Worker 3'},
            'prometheus': {'host': '127.0.0.1', 'port': 9096, 'name': 'Prometheus'},
            'grafana': {'host': '127.0.0.1', 'port': 3000, 'name': 'Grafana'},
            'cadvisor': {'host': '127.0.0.1', 'port': 8084, 'name': 'cAdvisor'},
            'alertmanager': {'host': '127.0.0.1', 'port': 59093, 'name': 'Alertmanager'},
            'schema-registry':{'host':'127.0.0.1','port':8082,'name':'Schema Registry'},
        }
        self.test_results: Dict[str,bool]={}
        self.kafka_config = {
            'bootstrap.servers':'127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095',
            'client.id': 'docker-server-tester',
            'request.timeout.ms': 5000,
            'socket.timeout.ms': 5000
        }
        self.kafka_producer_config = self.kafka_config.copy()
        self.kafka_producer_config.update({
            'acks': 'all',
            'retries': 3,
            'enable.idempotence': True
        })
    def check_port(self,host:str,port:int ,timeout:float=10.0) -> bool:

        """
        check if a port is open and accepting connection
        :param self:
        :param host:
        :param port:
        :param timeout:
        :return:
        """

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host,port))
            sock.close()
            return result == 0

        except Exception as e:
            logger.error(f"Error checking {host}:{port}:{e}")
            return False


    def check_service(self,service_name:str)-> bool:
        """
        check if a service is running
        :param self:
        :param service_name:
        :return:
        """

        service_info= self.services.get(service_name)
        if not service_info:
            logger.error(f"Service {service_name} not found")
            return False

        logger.info(f"Checking {service_info['name']} at {service_info['host']}:{service_info['port']}")

        is_running= self.check_port(service_info['host'],service_info['port'])

        self.test_results[service_name] = is_running

        status="Running" if is_running else "Not Running"
        logger.info(f"Service {service_info['name']} status: {status}")

        return is_running


    def check_all_ports(self) -> Dict[str, bool]:
        """
        check all service ports
        :param self:
        :return:
        """

        logger.info("Checking all ports")

        for service_name in self.services.keys():
            self.check_service(service_name)

        return self.test_results

    def test_kafka_connectivity(self) -> bool:
        logger.info("Testing kafka connectivity")
        test_topic= 'test-kafka-connectivity'
        test_message={
            'timestamp': datetime.now().isoformat(),
            'message': 'Hello from Confluent Kafka test!',
            'test_id': 'integration_test_001',
            'source': 'docker_service_test'
        }

        try:
            #create Admin client to create topic if needed
            admin_client = AdminClient(self.kafka_config)

            #check if topic exists and create one if it doesn't
            topic_metadata= admin_client.list_topics(timeout=10)
            if test_topic not in topic_metadata.topics:
                logger.info(f"Creating test topic: {test_topic}")
                new_topic= NewTopic(test_topic,num_partitions=3,replication_factor=3)

                futures= admin_client.create_topics([new_topic])

                for topic,future in futures.items():
                    try:
                        future.result(timeout=10)
                        logger.info(f"Topic '{topic}' created successfully")

                    except Exception as e:
                        logger.warning(f"Could not create topic: {e}")

            #test producer
            producer= Producer(self.kafka_producer_config)

            #send test message with delivery callback
            def delivery_callback(err,msg):
                if err:
                    logger.error(f"Message delivery failed: {err}")

                else:
                    logger.info(f"message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

            producer.produce(
                test_topic,
                key=str(test_message['test_id']).encode('utf-8'),
                value=json.dumps(test_message).encode('utf-8'),
                callback=delivery_callback
            )

            #flush with timeout
            remaining= producer.flush(10)
            if remaining > 0:
                logger.warning(f"{remaining} message were not delivered")
                return False

            logger.info("Successfully sent test message to kafka")

            #Test Consumer
            consumer_config= self.kafka_config.copy()
            consumer_config.update({
                'group.id': 'test-group-' + str(int(time.time())),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'session.timeout.ms': 10000,
                'max.poll.interval.ms': 30000,
            })

            consumer= Consumer(consumer_config)
            consumer.subscribe([test_topic])

            #poll for messages
            received_messages= []
            timeout_start = time.time()

            while time.time()-timeout_start < 10:
                msg = consumer.poll(timeout=10)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue

                    else:
                        logger.error(f"Consumer error: {msg.error()}")

                else:
                    try:
                        value = json.loads(msg.value().decode('utf-8'))
                        received_messages.append(value)
                        logger.info(f"Received message: {value}")

                        if len(received_messages) >=1:
                            break
                    except Exception as e:
                        logger.warning(f"could not decode message: {e}")

            consumer.close()

            if received_messages:
                logger.info(f"Successfully received {len(received_messages)} messages from kafka")
                return True

            else:
                logger.warning("No messages received from kafka within timeout")

                return False
        except Exception as e:
            logger.error(f"Kafka connectivity test failed: {e}")
            return False

    def test_kafka_admin_operations(self) -> bool:
        logger.info("testing Kafka admin operations")

        try:
            admin_client = AdminClient(self.kafka_config)

            #list topics
            topics= admin_client.list_topics(timeout=10)
            logger.info(f"Found {len(topics.topics)} topics")

            #test topic creation
            test_topic = f"Test-admin-{int(time.time())}"
            new_topic= NewTopic(test_topic,num_partitions=3,replication_factor=3)
            futures= admin_client.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result(timeout=10)
                    logger.info(f"successfully created topic: {topic}")
                except Exception as e:
                    logger.warning(f"Could not create topic: {e}")
                    return False

            #verify topi exists, retrying briefly to allow metadata propagation
            for attempt in range(5):
                topic_after = admin_client.list_topics(timeout=10)
                if test_topic in topic_after.topics:
                    logger.info(f"Topic {test_topic} already exists")
                    return True

                logger.error(f"Topic {test_topic} does not exist")
                time.sleep(2)

            logger.error(f"Topic {test_topic} does not exist after retries")
            return False
        except Exception as e:
            logger.error(f"Kafka admin operations test failed: {e}")
            return False

    def test_kafka_consumer_groups(self) -> bool:
        logger.info("Testing Kafka consumer groups")
        try:
            admin_client = AdminClient(self.kafka_config)

            #list consumer groups
            groups_future = admin_client.list_consumer_groups(request_timeout=10)
            groups_result = groups_future.result()
            logger.info(f"Found {len(groups_result.valid)} consumer groups")

            #test creating a consumer group ny consuming
            consumer_config = self.kafka_config.copy()
            consumer_config.update({
                'group.id': 'test-group-admin-' + str(int(time.time())),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            })

            #use test topics
            test_topic ='test-kafka-connectivity'

            #check if topic exists
            topics= admin_client.list_topics(timeout=10)
            if test_topic not in topics.topics:
                logger.warning(f"Topic '{test_topic}' does not exist, skipping consumer group")
                return True

            consumer= Consumer(consumer_config)
            consumer.subscribe([test_topic])

            #poll once to create the group
            msg=consumer.poll(timeout=10)
            consumer.close()

            #verify group exists
            group_after= admin_client.list_consumer_groups(request_timeout=10)
            group_after_result= group_after.result()
            group_exists= any(g.group_id == consumer_config['group.id'] for g in group_after_result.valid)

            if group_exists:
                logger.info(f" Consumer group {consumer_config['group.id']} already exists")
                return True
            else:
                logger.warning(f" Consumer group {consumer_config['group.id']} does not exist")
                return False

        except Exception as e:
            logger.error(f"Consumer group test failed: {e}")
            return False
    def test_spark_connectivity(self) -> bool:
        """
        Test spark session creation and basic operations
        :param self:
        :return:
        """

        logger.info("Testing spark session connectivity")
        try:
            #check is spark master is accessible
            if not self.check_port('127.0.0.1',7077):
                logger.error(' Spark master Port not available')
                return False

            #create sparksession
            spark=SparkSession.builder.appName('DockerServices') \
                    .master("local[*]")\
                    .config("spark.executor.memory","512m")\
                    .config("spark.executor.cores","1").getOrCreate()

            #test basic spark functionality
            test_data=[("test",1),("test2",2)]
            df = spark.createDataFrame(test_data,['name','value'])
            result= df.count()

            spark.stop()

            if result == 2:
                logger.info("Spark connectivity test successful")

                return True
            else:
                logger.error('Spark connectivity test failed')
                return False

        except Exception as e:
            logger.error(f"Spark connectivity test failed: {e}")
            return False

    def test_kafdrop_ui(self) -> bool:
        logger.info("Testing Kafdrop UI")

        try:
            response=requests.get("http://127.0.0.1:9000",timeout=20)
            if response.status_code == 200:
                logger.info("Kafdrop UI is accessible")
                #check if it shows kafka clusters
                if 'kafka' in response.text or 'kafka' in response.text.lower():
                    logger.info("Kafdrop is showing kafka cluster information")
                return True

            else:
                logger.warning('Kafdrop UI is not accessible')
                return False
        except Exception as e:
            logger.error(f"Kafdrop UI is not accessible: {e}")
            return False

    def test_prometheus_metrics(self) -> bool:
        logger.info("Testing Prometheus Metrics")
        try:
            response = requests.get('http://127.0.0.1:9096/api/v1/query',
                                    params={'query': 'up{job="kafka-exporter"}'},
                                    timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    results = data.get('data', {}).get('result', [])
                    up_count = sum(1 for r in results if r.get('value', [None, '0'])[1] == '1')
                    if up_count > 0:
                        logger.info("Prometheus metrics test successful")
                        return True
                    else:
                        logger.warning("No prometheus targets are up")
                        return False
                else:
                    logger.warning(f"prometheus API returned status: {data.get('status')}")
                    return False
            else:
                logger.warning(f"Prometheus returned status: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Prometheus metrics test failed: {e}")
            return False
    def test_grafana_login(self) -> bool:
        logger.info("Testing Grafana login")
        try:
            # Check if Grafana is accessible
            response = requests.get('http://127.0.0.1:3000', timeout=10)
            if response.status_code == 200:
                logger.info("Grafana is accessible")

                return True
            else:
                logger.warning(f"Grafana returned status code: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Grafana test failed: {e}")
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all service tests and return consolidated results"""
        logger.info("=" * 60)
        logger.info("STARTING DOCKER SERVICES INTEGRATION TESTS")
        logger.info("=" * 60)

        test_start_time = datetime.now()

        # Test all ports
        port_results = self.check_all_ports()

        # Run functional tests
        functional_tests = {
            'kafka_producer_consumer': self.test_kafka_connectivity(),
            'kafka_admin_operations': self.test_kafka_admin_operations(),
            'kafka_consumer_groups': self.test_kafka_consumer_groups(),
            'spark_functional': self.test_spark_connectivity(),
            'kafdrop_ui': self.test_kafdrop_ui(),
            'prometheus': self.test_prometheus_metrics(),
            'grafana': self.test_grafana_login()
        }

        test_end_time = datetime.now()
        duration = (test_end_time - test_start_time).total_seconds()

        # Summary
        results = {
            'timestamp': test_start_time.isoformat(),
            'duration_seconds': duration,
            'port_checks': port_results,
            'functional_tests': functional_tests,
            'summary': {
                'total_services': len(self.services),
                'services_running': sum(1 for v in port_results.values() if v),
                'functional_tests_passed': sum(1 for v in functional_tests.values() if v),
                'functional_tests_total': len(functional_tests),
                'all_passed': all(list(port_results.values()) + list(functional_tests.values()))
            }
        }

        # Print summary
        logger.info("TEST SUMMARY")
        logger.info(f"Total Services: {results['summary']['total_services']}")
        logger.info(f"Running Services: {results['summary']['services_running']}")
        logger.info(
            f"Functional Tests Passed: {results['summary']['functional_tests_passed']}/{results['summary']['functional_tests_total']}")
        logger.info(f"All Tests Passed: {'YES' if results['summary']['all_passed'] else 'NO'}")
        logger.info(f"Test Duration: {duration:.2f} seconds")


        return results

class TestDockerServices:
    """Pytest test class for Docker services"""

    @classmethod
    def setup_class(cls):
        cls.tester = DockerServiceTesters()

    def test_all_ports(self):
        """Test if all service ports are accessible"""
        results = self.tester.check_all_ports()
        # Critical services that must be running
        critical_services = ['kafka1', 'kafka2', 'kafka3', 'kafka4', 'spark-master', 'prometheus']
        assert all(results.get(s, False) for s in critical_services), f"Critical services not running: {results}"

    def test_kafka_functional(self):
        """Test Kafka producer/consumer functionality"""
        assert self.tester.test_kafka_connectivity(), "Kafka functional test failed"

    def test_kafka_admin(self):
        """Test Kafka admin operations"""
        assert self.tester.test_kafka_admin_operations(), "Kafka admin test failed"

    def test_kafka_consumer_groups(self):
        """Test Kafka consumer groups"""
        assert self.tester.test_kafka_consumer_groups(), "Kafka consumer group test failed"

    def test_spark_functional(self):
        """Test Spark connectivity"""
        assert self.tester.test_spark_connectivity(), "Spark functional test failed"

    def test_kafdrop_ui(self):
        """Test Kafdrop UI"""
        assert self.tester.test_kafdrop_ui(), "Kafdrop UI test failed"

    def test_prometheus_metrics(self):
        """Test Prometheus metrics"""
        assert self.tester.test_prometheus_metrics(), "Prometheus test failed"

    def test_grafana_login(self):
        """Test Grafana login"""
        assert self.tester.test_grafana_login(), "Grafana test failed"

# Standalone execution
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Test Docker Compose services')
    parser.add_argument('--service', type=str, help='Test specific service')
    parser.add_argument('--all', action='store_true', default=True, help='Test all services')
    parser.add_argument('--save-results', action='store_true', help='Save results to file')
    args = parser.parse_args()

    tester = DockerServiceTesters()

    if args.service:
        if args.service in tester.services:
            result = tester.check_service(args.service)
            if args.service.startswith('kafka'):
                tester.test_kafka_connectivity()
            elif args.service == 'spark_master':
                tester.test_spark_connectivity()
            elif args.service == 'kafdrop':
                tester.test_kafdrop_ui()
            elif args.service == 'prometheus':
                tester.test_prometheus_metrics()
            elif args.service == 'grafana':
                tester.test_grafana_login()
        else:
            logger.error(f"Unknown service: {args.service}")
            sys.exit(1)
    else:
        results = tester.run_all_tests()

        # Save results to file
        if args.save_results:
            with open('docker_test_results.json', 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info("Test results saved to docker_test_results.json")

        # Exit with appropriate code
        if results['summary']['all_passed']:
            logger.info("ALL TESTS PASSED!")
            sys.exit(0)
        else:
            logger.error("SOME TESTS FAILED! Check the logs above.")
            sys.exit(1)