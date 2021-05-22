docker container stop localstack
docker container rm localstack

docker run --rm -p 4566:4566 -p 4571:4571 -p 8080:8080 -d localstack/localstack-full -e START_WEB=1 --name localstack

sleep 10

export AWS_ACCESS_KEY_ID=foobar
export AWS_SECRET_ACCESS_KEY=foobar

aws --endpoint-url=http://localhost:4566 s3 mb s3://crime-data
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket crime-data --acl public-read

aws --endpoint-url=http://localhost:4566 s3 cp "data/" s3://crime-data/ --recursive --exclude "*" --include "*.csv"