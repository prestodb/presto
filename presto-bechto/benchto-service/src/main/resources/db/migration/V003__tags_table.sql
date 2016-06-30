CREATE TABLE tags
(
  id             BIGSERIAL PRIMARY KEY NOT NULL,
  name           VARCHAR(255)          NOT NULL,
  description    VARCHAR(1024)                 ,
  created        TIMESTAMP             NOT NULL,
  environment_id BIGINT                NOT NULL
);

ALTER TABLE tags ADD FOREIGN KEY (environment_id) REFERENCES environments (id);

CREATE INDEX idx_tags_environment_id ON tags (environment_id);
CREATE INDEX idx_tags_created ON tags (created);
