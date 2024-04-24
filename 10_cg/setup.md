- Install Java
- Install Flink 1.18 [link](https://flink.apache.org/downloads/)
```bash
tar -zxvf flink-1.18.1-bin-scala_2.12.tgz
mv flink-1.18.1 ~/bin/flink-1.18.1
```
- Start local cluster (local development only)
```bash
~/bin/flink-1.18.1/bin/start-cluster.sh
# Flink dashboard will be available on http://localhost:8081/
```
- Install and start kinesalite - aws kinesis local implementation
```bash
sudo apt update
sudo apt upgrade
sudo apt install nodejs npm
sudo npm install -g kinesalite
kinesalite
```
- Create the required stream in kinesalite
```bash
# potentially delete old stream if you want to clear out the data
AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws kinesis  --endpoint-url http://localhost:4567 --region us-east-1 delete-stream --stream-name flink-test
AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws kinesis  --endpoint-url http://localhost:4567 --region us-east-1 create-stream --stream-name flink-test --shard-count 1
AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws kinesis list-streams --endpoint-url http://localhost:4567 --region us-east-1
```
- Start dynamodb through docker
```bash
docker compose up
sudo chmod 777 ./docker/dynamodb
```
- Create the dynamodb tables for features and candidates
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb create-table \
    --table-name flink-test \
    --attribute-definitions \
        AttributeName=key,AttributeType=S \
				AttributeName=featureName,AttributeType=S \
		--key-schema \
		        AttributeName=key,KeyType=HASH \
		        AttributeName=featureName,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
		--endpoint-url http://localhost:8000
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb create-table \
    --table-name candidates \
    --attribute-definitions \
        AttributeName=CGName,AttributeType=S \
		--key-schema \
		        AttributeName=CGName,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
		--endpoint-url http://localhost:8000
```
- Build the job
```bash
./gradlew jar -Pconfig=debug.yaml
```
- Submit the job to local Flink
```bash
~/bin/flink-1.18.1/bin/flink run app/build/libs/app.jar
```
- produce some messages to kinesis source. Better run a few times with a little delay to make sure events get in different aggregation windows
```bash
bash produce_local.sh
```
- Read results in dynamodb
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb --endpoint-url http://localhost:8000 scan --table-name flink-test
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb --endpoint-url http://localhost:8000 scan --table-name candidates
```
