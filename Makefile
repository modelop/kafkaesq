
.PHONY: default

default:
	sbt compile assembly
	cat stub.sh target/scala-2.10/kafkaesq.jar > kafkaesq
	chmod +x kafkaesq
	aws s3 cp --acl public-read kafkaesq s3://kafkaesq/

