CREATE TABLE t_Users (
    id BIGINT,
    rating INT,
    subscription Boolean
) WITH (
   'connector'='datagen',
   'rows-per-second'='500',
   'number-of-rows'='10000',
   'fields.id.min'='0',
   'fields.rating.min'='1',
   'fields.rating.max'='100'
);

CREATE TABLE t_UserOutput(
    rating INT,
    subscription Boolean
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'user_data'
);

INSERT INTO t_UserOutput
SELECT rating, subscription
FROM t_Users;