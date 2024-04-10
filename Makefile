format:
	poetry run ruff format .

check:
	poetry run ruff check .
	poetry run mypy . --ignore-missing-imports

fix:
	poetry run ruff --fix .

format-fix:
	poetry run ruff format . 
	poetry run ruff --fix .

make-bucket:
	aws --endpoint-url=http://localhost:4566 s3api create-bucket --bucket data
	aws --endpoint-url=http://localhost:4566 s3 cp ./data/raw/OnlineRetail.csv s3://data/raw/OnlineRetail.csv

copy-fake-transactions:
	aws --endpoint-url=http://localhost:4566 s3 cp ./data/raw/FakeTransactionData0.csv s3://data/raw/FakeTransactionData0.csv
	aws --endpoint-url=http://localhost:4566 s3 cp ./data/raw/FakeTransactionData1.csv s3://data/raw/FakeTransactionData1.csv
	aws --endpoint-url=http://localhost:4566 s3 cp ./data/raw/FakeTransactionData2.csv s3://data/raw/FakeTransactionData2.csv

start-cluster-3.5.0:
	../spark-3.5.0-bin-hadoop3/sbin/start-history-server.sh
	../spark-3.5.0-bin-hadoop3/sbin/start-master.sh
	../spark-3.5.0-bin-hadoop3/sbin/start-worker.sh spark://nikolina:7077 -c 4 -m 4GB &
	../spark-3.5.0-bin-hadoop3/sbin/start-worker.sh spark://nikolina:7077 -c 4 -m 4GB &

stop-cluster-3.5.0:
	../spark-3.5.0-bin-hadoop3/sbin/stop-history-server.sh
	../spark-3.5.0-bin-hadoop3/sbin/stop-master.sh
	../spark-3.5.0-bin-hadoop3/sbin/stop-worker.sh spark://nikolina:7077 &
	../spark-3.5.0-bin-hadoop3/sbin/stop-worker.sh spark://nikolina:7077 &

start-cluster-3.4.2:
	../spark-3.4.2-bin-hadoop3/sbin/start-history-server.sh
	../spark-3.4.2-bin-hadoop3/sbin/start-master.sh
	../spark-3.4.2-bin-hadoop3/sbin/start-worker.sh spark://nikolina:7077 -c 4 -m 4GB &
	../spark-3.4.2-bin-hadoop3/sbin/start-worker.sh spark://nikolina:7077 -c 4 -m 4GB &

stop-cluster-3.4.2:
	../spark-3.4.2-bin-hadoop3/sbin/stop-history-server.sh
	../spark-3.4.2-bin-hadoop3/sbin/stop-master.sh
	../spark-3.4.2-bin-hadoop3/sbin/stop-worker.sh spark://nikolina:7077 &
	../spark-3.4.2-bin-hadoop3/sbin/stop-worker.sh spark://nikolina:7077 &